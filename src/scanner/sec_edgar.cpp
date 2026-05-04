#include "scanner/sec_edgar.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"
#include "json.hpp"

#include <cctype>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// SEC EDGAR scanners.
//
// EDGAR requires a descriptive User-Agent header per its access policy.
// Configure via:
//   SET sec_user_agent = 'Your Name your.email@example.com';
// A default value is supplied so basic queries work, but production
// users should set their own to avoid rate-limit issues.
//
// sec_filings(cik_or_ticker [, filing_type [, limit]])
//   → (accession_number VARCHAR, filing_date DATE, report_date DATE,
//      form VARCHAR, primary_document VARCHAR, url VARCHAR)
//
// sec_facts(cik_or_ticker, concept [, taxonomy])
//   → (concept VARCHAR, unit VARCHAR, value DOUBLE,
//      period_start DATE, period_end DATE, fiscal_year INTEGER,
//      fiscal_period VARCHAR, form VARCHAR, filed DATE)
//
// `concept` examples: 'Revenues', 'NetIncomeLoss', 'Assets', 'Liabilities'.
// taxonomy defaults to 'us-gaap'.
// ──────────────────────────────────────────────────────────────

namespace {

using nlohmann::json;

static string PadCIK(const string &cik) {
	string digits;
	for (char c : cik) {
		if (std::isdigit((unsigned char)c)) {
			digits.push_back(c);
		}
	}
	if (digits.empty()) {
		throw InvalidInputException("CIK must contain at least one digit");
	}
	while (digits.size() < 10) {
		digits = "0" + digits;
	}
	return digits;
}

static bool LooksLikeCIK(const string &s) {
	for (char c : s) {
		if (std::isdigit((unsigned char)c)) {
			continue;
		}
		if (c == 'C' || c == 'I' || c == 'K' || c == 'c' || c == 'i' || c == 'k') {
			continue;
		}
		return false;
	}
	return !s.empty();
}

static string GetUserAgent(ClientContext &context) {
	Value v;
	if (context.TryGetCurrentSetting("sec_user_agent", v) && !v.IsNull()) {
		auto s = v.GetValue<string>();
		if (!s.empty()) {
			return s;
		}
	}
	return "Scrooge-McDuck DuckDB Extension scrooge-mcduck@example.com";
}

// Perform an HTTPS GET against an SEC host with a proper User-Agent.
static string HttpGet(const string &url, const string &user_agent) {
	const string scheme = "https://";
	if (url.compare(0, scheme.size(), scheme) != 0) {
		throw InvalidInputException("only https:// URLs supported: %s", url);
	}
	auto rest = url.substr(scheme.size());
	auto slash = rest.find('/');
	string host = slash == string::npos ? rest : rest.substr(0, slash);
	string path = slash == string::npos ? "/" : rest.substr(slash);

	duckdb_httplib_openssl::SSLClient client(host);
	client.enable_server_certificate_verification(false);
	client.set_follow_location(true);
	client.set_connection_timeout(30);
	client.set_read_timeout(60);

	duckdb_httplib_openssl::Headers headers = {
	    {"User-Agent", user_agent},
	    {"Accept", "application/json"},
	    {"Host", host},
	};
	auto res = client.Get(path.c_str(), headers);
	if (!res) {
		throw IOException("request to %s failed: %s", url,
		                   duckdb_httplib_openssl::to_string(res.error()));
	}
	if (res->status != 200) {
		throw IOException("request to %s returned status %d", url, res->status);
	}
	return res->body;
}

static string ResolveCIK(ClientContext &context, const string &input) {
	if (LooksLikeCIK(input)) {
		return PadCIK(input);
	}
	string ticker_upper;
	for (char c : input) {
		ticker_upper.push_back((char)std::toupper((unsigned char)c));
	}
	auto body = HttpGet("https://www.sec.gov/files/company_tickers_exchange.json", GetUserAgent(context));
	auto j = json::parse(body);
	if (!j.contains("data") || !j["data"].is_array()) {
		throw IOException("unexpected SEC ticker file shape");
	}
	for (auto &row : j["data"]) {
		if (!row.is_array() || row.size() < 4) {
			continue;
		}
		string ticker = row[2].is_string() ? row[2].get<string>() : "";
		string upper;
		for (char c : ticker) {
			upper.push_back((char)std::toupper((unsigned char)c));
		}
		if (upper == ticker_upper) {
			int64_t cik = row[0].is_number_integer() ? row[0].get<int64_t>() : 0;
			return PadCIK(std::to_string(cik));
		}
	}
	throw InvalidInputException("ticker '%s' not found in SEC company list", input);
}

static date_t ParseDate(const string &s) {
	if (s.empty()) {
		return date_t();
	}
	try {
		return Date::FromString(s);
	} catch (...) {
		return date_t();
	}
}

} // namespace

// ─── sec_filings ─────────────────────────────────────────────────

struct SecFilingsData : public TableFunctionData {
	struct Row {
		string accession;
		date_t filing_date;
		date_t report_date;
		bool report_date_valid = false;
		string form;
		string primary_doc;
		string url;
	};
	vector<Row> rows;
	idx_t cur = 0;
};

static unique_ptr<FunctionData> FilingsBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto raw = input.inputs[0].GetValue<string>();
	string filter_form = input.inputs.size() > 1 && !input.inputs[1].IsNull()
	                         ? input.inputs[1].GetValue<string>() : "";
	int64_t limit = input.inputs.size() > 2 && !input.inputs[2].IsNull()
	                    ? input.inputs[2].GetValue<int64_t>() : 40;

	auto cik_padded = ResolveCIK(context, raw);
	int64_t cik_int = std::stoll(cik_padded);
	auto ua = GetUserAgent(context);
	auto body = HttpGet("https://data.sec.gov/submissions/CIK" + cik_padded + ".json", ua);
	auto j = json::parse(body);

	auto data = make_uniq<SecFilingsData>();
	if (j.contains("filings") && j["filings"].contains("recent")) {
		auto &r = j["filings"]["recent"];
		auto get_arr = [&](const char *k) -> const json & {
			static const json empty = json::array();
			return r.contains(k) ? r[k] : empty;
		};
		auto &acc = get_arr("accessionNumber");
		auto &fd = get_arr("filingDate");
		auto &rd = get_arr("reportDate");
		auto &fm = get_arr("form");
		auto &pd = get_arr("primaryDocument");
		idx_t n = acc.size();
		for (idx_t i = 0; i < n; i++) {
			string form = i < fm.size() && fm[i].is_string() ? fm[i].get<string>() : "";
			if (!filter_form.empty() && form != filter_form) {
				continue;
			}
			SecFilingsData::Row row;
			row.accession = i < acc.size() && acc[i].is_string() ? acc[i].get<string>() : "";
			row.filing_date = ParseDate(i < fd.size() && fd[i].is_string() ? fd[i].get<string>() : "");
			string rds = i < rd.size() && rd[i].is_string() ? rd[i].get<string>() : "";
			if (!rds.empty()) {
				row.report_date = ParseDate(rds);
				row.report_date_valid = true;
			}
			row.form = form;
			row.primary_doc = i < pd.size() && pd[i].is_string() ? pd[i].get<string>() : "";
			string acc_path = row.accession;
			acc_path.erase(std::remove(acc_path.begin(), acc_path.end(), '-'), acc_path.end());
			row.url = "https://www.sec.gov/Archives/edgar/data/" + std::to_string(cik_int) + "/" +
			          acc_path + "/" + row.primary_doc;
			data->rows.push_back(std::move(row));
			if ((int64_t)data->rows.size() >= limit) {
				break;
			}
		}
	}

	names = {"accession_number", "filing_date", "report_date", "form", "primary_document", "url"};
	return_types = {LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(data);
}

static void FilingsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (SecFilingsData &)*input.bind_data;
	auto acc = FlatVector::GetData<string_t>(output.data[0]);
	auto fd = FlatVector::GetData<date_t>(output.data[1]);
	auto rd = FlatVector::GetData<date_t>(output.data[2]);
	auto &rd_valid = FlatVector::Validity(output.data[2]);
	auto fm = FlatVector::GetData<string_t>(output.data[3]);
	auto pd = FlatVector::GetData<string_t>(output.data[4]);
	auto url = FlatVector::GetData<string_t>(output.data[5]);

	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && d.cur < d.rows.size()) {
		auto &row = d.rows[d.cur];
		acc[out] = StringVector::AddString(output.data[0], row.accession);
		fd[out] = row.filing_date;
		if (row.report_date_valid) {
			rd[out] = row.report_date;
		} else {
			rd_valid.SetInvalid(out);
		}
		fm[out] = StringVector::AddString(output.data[3], row.form);
		pd[out] = StringVector::AddString(output.data[4], row.primary_doc);
		url[out] = StringVector::AddString(output.data[5], row.url);
		out++;
		d.cur++;
	}
	output.SetCardinality(out);
}

// ─── sec_facts ───────────────────────────────────────────────────

struct SecFactsData : public TableFunctionData {
	struct Row {
		string concept;
		string unit;
		double value;
		date_t period_start;
		bool period_start_valid = false;
		date_t period_end;
		bool period_end_valid = false;
		int32_t fiscal_year;
		bool fiscal_year_valid = false;
		string fiscal_period;
		string form;
		date_t filed;
		bool filed_valid = false;
	};
	vector<Row> rows;
	idx_t cur = 0;
};

static unique_ptr<FunctionData> FactsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto raw = input.inputs[0].GetValue<string>();
	auto concept = input.inputs[1].GetValue<string>();
	auto taxonomy = input.inputs.size() > 2 && !input.inputs[2].IsNull()
	                    ? input.inputs[2].GetValue<string>() : "us-gaap";

	auto cik_padded = ResolveCIK(context, raw);
	auto ua = GetUserAgent(context);
	auto body = HttpGet("https://data.sec.gov/api/xbrl/companyconcept/CIK" + cik_padded +
	                       "/" + taxonomy + "/" + concept + ".json", ua);
	auto j = json::parse(body);

	auto data = make_uniq<SecFactsData>();
	if (j.contains("units") && j["units"].is_object()) {
		for (auto &unit_kv : j["units"].items()) {
			const string unit = unit_kv.key();
			if (!unit_kv.value().is_array()) {
				continue;
			}
			for (auto &fact : unit_kv.value()) {
				SecFactsData::Row row;
				row.concept = concept;
				row.unit = unit;
				if (fact.contains("val") && fact["val"].is_number()) {
					row.value = fact["val"].get<double>();
				} else {
					continue;
				}
				if (fact.contains("start") && fact["start"].is_string()) {
					row.period_start = ParseDate(fact["start"].get<string>());
					row.period_start_valid = true;
				}
				if (fact.contains("end") && fact["end"].is_string()) {
					row.period_end = ParseDate(fact["end"].get<string>());
					row.period_end_valid = true;
				}
				if (fact.contains("fy") && fact["fy"].is_number_integer()) {
					row.fiscal_year = fact["fy"].get<int32_t>();
					row.fiscal_year_valid = true;
				}
				if (fact.contains("fp") && fact["fp"].is_string()) {
					row.fiscal_period = fact["fp"].get<string>();
				}
				if (fact.contains("form") && fact["form"].is_string()) {
					row.form = fact["form"].get<string>();
				}
				if (fact.contains("filed") && fact["filed"].is_string()) {
					row.filed = ParseDate(fact["filed"].get<string>());
					row.filed_valid = true;
				}
				data->rows.push_back(std::move(row));
			}
		}
	}
	// Sort by filed desc for stable output.
	std::sort(data->rows.begin(), data->rows.end(),
	          [](const SecFactsData::Row &a, const SecFactsData::Row &b) {
		          return a.filed.days > b.filed.days;
	          });

	names = {"concept", "unit", "value", "period_start", "period_end",
	         "fiscal_year", "fiscal_period", "form", "filed"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::DATE, LogicalType::DATE, LogicalType::INTEGER,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DATE};
	return std::move(data);
}

static void FactsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (SecFactsData &)*input.bind_data;
	auto concept = FlatVector::GetData<string_t>(output.data[0]);
	auto unit = FlatVector::GetData<string_t>(output.data[1]);
	auto value = FlatVector::GetData<double>(output.data[2]);
	auto pstart = FlatVector::GetData<date_t>(output.data[3]);
	auto &pstart_v = FlatVector::Validity(output.data[3]);
	auto pend = FlatVector::GetData<date_t>(output.data[4]);
	auto &pend_v = FlatVector::Validity(output.data[4]);
	auto fy = FlatVector::GetData<int32_t>(output.data[5]);
	auto &fy_v = FlatVector::Validity(output.data[5]);
	auto fp = FlatVector::GetData<string_t>(output.data[6]);
	auto form = FlatVector::GetData<string_t>(output.data[7]);
	auto filed = FlatVector::GetData<date_t>(output.data[8]);
	auto &filed_v = FlatVector::Validity(output.data[8]);

	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && d.cur < d.rows.size()) {
		auto &row = d.rows[d.cur];
		concept[out] = StringVector::AddString(output.data[0], row.concept);
		unit[out] = StringVector::AddString(output.data[1], row.unit);
		value[out] = row.value;
		if (row.period_start_valid) {
			pstart[out] = row.period_start;
		} else {
			pstart_v.SetInvalid(out);
		}
		if (row.period_end_valid) {
			pend[out] = row.period_end;
		} else {
			pend_v.SetInvalid(out);
		}
		if (row.fiscal_year_valid) {
			fy[out] = row.fiscal_year;
		} else {
			fy_v.SetInvalid(out);
		}
		fp[out] = StringVector::AddString(output.data[6], row.fiscal_period);
		form[out] = StringVector::AddString(output.data[7], row.form);
		if (row.filed_valid) {
			filed[out] = row.filed;
		} else {
			filed_v.SetInvalid(out);
		}
		out++;
		d.cur++;
	}
	output.SetCardinality(out);
}

void RegisterSecEdgarScanner(Connection &conn, Catalog &catalog) {
	auto &config = DBConfig::GetConfig(*conn.context->db);
	config.AddExtensionOption(
	    "sec_user_agent",
	    "User-Agent header sent to SEC EDGAR (the SEC requires a descriptive UA)",
	    LogicalType::VARCHAR,
	    "Scrooge-McDuck DuckDB Extension scrooge-mcduck@example.com");

	{
		TableFunctionSet set("sec_filings");
		set.AddFunction(TableFunction({LogicalType::VARCHAR}, FilingsScan, FilingsBind));
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, FilingsScan, FilingsBind));
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
		                               FilingsScan, FilingsBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
	{
		TableFunctionSet set("sec_facts");
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, FactsScan, FactsBind));
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
		                               FactsScan, FactsBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
