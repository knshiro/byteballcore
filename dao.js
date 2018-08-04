var db = require('./db');

// ############
// balances.js
// ############

function isWalletAddress(wallet) {
	return typeof wallet === 'string' && wallet.length === 32; // ValidationUtils.isValidAddress
}

// readOutputsBalance - start

function readBalances(wallet, options, callback) {

	var perAddress = !!options.perAddress;
	var outputsOnly = !!options.outputsOnly;

	var whereCondition = "";
	var joinMyAddresses = "";
	var using = "";
	var myAddressesJoin = "";
	if (typeof wallet === 'array') {
		var strAddressList = arrSharedAddresses.map(db.escape).join(', ');
		whereCondition = "address IN (" + strAddressList + ")";
	}
	else if (isWalletAddress(wallet)) {
		whereCondition = "address=" + db.escape(wallet);
	}
	else {
		whereCondition = "wallet=" + db.escape(wallet);
		using = "USING(address)";
		joinMyAddresses = "JOIN my_addresses " + using;
		myAddressesJoin = "my_addresses CROSS JOIN";
	}

	var query =
		"SELECT asset, " + (perAddress ? "address, " : "" ) + "is_stable, SUM(amount) AS balance \n\
		FROM outputs " + joinMyAddresses + " CROSS JOIN units USING(unit) \n\
		WHERE is_spent=0 AND sequence='good' AND " + whereCondition + " \n\
		GROUP BY asset, " + (perAddress ? "address, " : "" ) + " is_stable ";

	if (!outputsOnly) {
		query +=
			"UNION ALL \n\
			SELECT NULL AS asset, address, 1 AS is_stable, SUM(amount) AS balance \n\
			FROM " + myAddressesJoin + " witnessing_outputs " + using + " \n\
			WHERE is_spent=0 AND " + whereCondition + (perAddress ? " GROUP BY address" : "") +" \n\
			UNION ALL \n\
			SELECT NULL AS asset, " + (perAddress ? "address, " : "" ) + "1 AS is_stable, SUM(amount) AS balance \n\
			FROM " + myAddressesJoin + " headers_commission_outputs " + using + " \n\
			WHERE is_spent=0 AND " + whereCondition + (perAddress ? " GROUP BY address" : "")
	}

	db.query(query, callback);
}

function listAllAssetsForWallet(wallet, callback) {
	var walletIsAddress = isWalletAddress(wallet);
	var join_my_addresses = walletIsAddress ? "" : "JOIN my_addresses USING(address)";
	var where_condition = walletIsAddress ? "address=?" : "wallet=?";

	db.query(
		"SELECT DISTINCT outputs.asset, is_private \n\
		FROM outputs "+join_my_addresses+" \n\
		CROSS JOIN units USING(unit) \n\
		LEFT JOIN assets ON outputs.asset=assets.unit \n\
		WHERE "+where_condition+" AND sequence='good'",
		[wallet],
		callback)
}

function readSharedAddressesOnWallet(wallet, callback){
	db.query("SELECT DISTINCT shared_address FROM my_addresses JOIN shared_address_signing_paths USING(address) WHERE wallet=?", [wallet], callback);
}

function readSharedAddressesDependingOnAddresses(arrMemberAddresses, callback){
	var strAddressList = arrMemberAddresses.map(db.escape).join(', ');
	db.query("SELECT DISTINCT shared_address FROM shared_address_signing_paths WHERE address IN("+strAddressList+")", callback);
}

// readSharedBalance - end

// ############
// catchup.js
// ############

function unitsFromMainChainAtIndex(last_known_mci, callback) {
	db.query(
		"SELECT is_stable FROM units WHERE is_on_main_chain=1 AND main_chain_index=?",
		[last_known_mci],
		callback);
}

function anyCatchupChainBalls(callback) {
	db.query("SELECT 1 FROM catchup_chain_balls LIMIT 1", function(rows){
		callback(rows.length > 0)
	});
}

function getUnitsForBall(ball, callback) {
	db.query(
		"SELECT is_stable, is_on_main_chain, main_chain_index FROM balls JOIN units USING(unit) WHERE ball=?",
		[ball],
		callback
	);
}

function getUnitsForBalls(balls, callback) {

	db.query(
		"SELECT is_stable, is_on_main_chain, main_chain_index, ball \n\
		FROM balls JOIN units USING(unit) \n\
		WHERE ball IN(?)",
		[balls],
		callback
	)
}

function findUnitsBetweenMcis(fromMci, toMci, callback) {
	var op = (fromMci === 0) ? ">=" : ">"; // if starting from 0, add genesis itself
	db.query(
		"SELECT unit, ball, content_hash FROM units LEFT JOIN balls USING(unit) \n\
		WHERE main_chain_index "+op+" ? AND main_chain_index<=? ORDER BY main_chain_index, `level`",
		[fromMci, toMci],
		callback
	);
}

function findUnitBallParents(unit, callback) {
	db.query(
		"SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=balls.unit WHERE child_unit=? ORDER BY ball",
		[unit],
		callback
	);
}

function findSkipListUnits(unit, callback) {
	db.query(
		"SELECT ball FROM skiplist_units LEFT JOIN balls ON skiplist_unit=balls.unit WHERE skiplist_units.unit=? ORDER BY ball",
		[unit],
		callback
	);
}

// HashTree Balls
function anyHashTreeBalls(callback) {
	db.query("SELECT 1 FROM hash_tree_balls LIMIT 1", function(rows){
		callback(rows.length > 0)
	});
}

function addHashTreeBall(objBall, callback) {
	db.query(
		"INSERT "+db.getIgnore()+" INTO hash_tree_balls (ball, unit) VALUES(?,?)",
		[objBall.ball, objBall.unit],
		callback)
}

function findBallsInHashTree(balls, callback) {
	db.query("SELECT ball FROM hash_tree_balls WHERE ball IN(?)",
	balls,
	callback)
}

function findBallsInHashTreeAndInBalls(balls, callback) {
	db.query(
		"SELECT ball FROM hash_tree_balls WHERE ball IN(?) UNION SELECT ball FROM balls WHERE ball IN(?)",
		[balls, balls],
		callback)
}

function deleteHashTreeBalls(balls, callback) {
	db.query("DELETE FROM hash_tree_balls WHERE ball IN(?)", [balls], callback);
}

function purgeHandledBallsFromHashTree(callback){
	db.query("SELECT ball FROM hash_tree_balls CROSS JOIN balls USING(ball)", function(rows){
		if (rows.length === 0)
			return callback();
		var arrHandledBalls = rows.map(function(row){ return row.ball; });
		dao.deleteHashTreeBalls(arrHandledBalls, callback)
	});
}

// Catchup chain balls
function createCatchupChainBalls(chainBalls, callback) {
	var arrValues = chainBalls.map(function (ball) {
		return "(" + db.escape(ball) + ")";
	});
	db.query(
		"INSERT INTO catchup_chain_balls (ball) VALUES " + arrValues.join(', '),
		callback
	);
}

function getCatchupBalls(limit, callback) {
	db.query(
		"SELECT ball, main_chain_index \n\
		FROM catchup_chain_balls LEFT JOIN balls USING(ball) LEFT JOIN units USING(unit) \n\
		ORDER BY member_index LIMIT ?", [limit], callback)
}

function deleteCatchupChainBalls(balls, callback) {
	db.query("DELETE FROM catchup_chain_balls WHERE ball IN (?)", [balls], callback)
}

