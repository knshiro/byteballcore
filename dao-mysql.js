var db = require('./db');
var dao = require('./dao');

function createHeadersCommissionContributions(since_mc_index, cb) {
	var best_child_sql = "SELECT unit \n\
			FROM parenthoods \n\
			JOIN units AS alt_child_units ON parenthoods.child_unit=alt_child_units.unit \n\
			WHERE parent_unit=punits.unit AND alt_child_units.main_chain_index-punits.main_chain_index<=1 AND +alt_child_units.sequence='good' \n\
			ORDER BY SHA1(CONCAT(alt_child_units.unit, next_mc_units.unit)) \n\
			LIMIT 1";
	// headers commissions to single unit author
	conn.query(
		"INSERT INTO headers_commission_contributions (unit, address, amount) \n\
			SELECT punits.unit, address, punits.headers_commission AS hc \n\
			FROM units AS chunits \n\
			JOIN unit_authors USING(unit) \n\
			JOIN parenthoods ON chunits.unit=parenthoods.child_unit \n\
			JOIN units AS punits ON parenthoods.parent_unit=punits.unit \n\
			JOIN units AS next_mc_units ON next_mc_units.is_on_main_chain=1 AND next_mc_units.main_chain_index=punits.main_chain_index+1 \n\
			WHERE chunits.is_stable=1 \n\
				AND +chunits.sequence='good' \n\
				AND punits.main_chain_index>? \n\
				AND chunits.main_chain_index-punits.main_chain_index<=1 \n\
				AND +punits.sequence='good' \n\
				AND punits.is_stable=1 \n\
				AND next_mc_units.is_stable=1 \n\
				AND chunits.unit=( " + best_child_sql + " ) \n\
				AND (SELECT COUNT(*) FROM unit_authors WHERE unit=chunits.unit)=1 \n\
				AND (SELECT COUNT(*) FROM earned_headers_commission_recipients WHERE unit=chunits.unit)=0 \n\
			UNION ALL \n\
			SELECT punits.unit, earned_headers_commission_recipients.address, \n\
				ROUND(punits.headers_commission*earned_headers_commission_share/100.0) AS hc \n\
			FROM units AS chunits \n\
			JOIN earned_headers_commission_recipients USING(unit) \n\
			JOIN parenthoods ON chunits.unit=parenthoods.child_unit \n\
			JOIN units AS punits ON parenthoods.parent_unit=punits.unit \n\
			JOIN units AS next_mc_units ON next_mc_units.is_on_main_chain=1 AND next_mc_units.main_chain_index=punits.main_chain_index+1 \n\
			WHERE chunits.is_stable=1 \n\
				AND +chunits.sequence='good' \n\
				AND punits.main_chain_index>? \n\
				AND chunits.main_chain_index-punits.main_chain_index<=1 \n\
				AND +punits.sequence='good' \n\
				AND punits.is_stable=1 \n\
				AND next_mc_units.is_stable=1 \n\
				AND chunits.unit=( " + best_child_sql + " )",
		[since_mc_index, since_mc_index],
		cb
	);

}