var db = require('./db');
var conf = require('./conf');

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


// ################
// witness_proof.js
// ################

function unstableMCUnitsFromIndex(mci, callback) {
	db.query(
		"SELECT unit FROM units WHERE +is_on_main_chain=1 AND main_chain_index>? ORDER BY main_chain_index DESC",
		[mci],
		callback)
}

// select the newest last ball unit
function getNewestUnitAmongst(units, callback) {
	db.query(
		"SELECT unit, main_chain_index FROM units WHERE unit IN(?) ORDER BY main_chain_index DESC LIMIT 1",
		units,
		callback
	)
}


// add definition changes and new definitions of witnesses
function witnessChangeAndDefinitionUnits(witnessAddresses, last_stable_mci, callback) {
	var after_last_stable_mci_cond = (last_stable_mci > 0) ? "latest_included_mc_index>="+last_stable_mci : "1";
	db.query(
		/*"SELECT DISTINCT units.unit \n\
		FROM unit_authors \n\
		JOIN units USING(unit) \n\
		LEFT JOIN address_definition_changes \n\
			ON units.unit=address_definition_changes.unit AND unit_authors.address=address_definition_changes.address \n\
		WHERE unit_authors.address IN(?) AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
			AND (unit_authors.definition_chash IS NOT NULL OR address_definition_changes.unit IS NOT NULL) \n\
		ORDER BY `level`",
		[arrWitnesses],*/
		// 1. initial definitions
		// 2. address_definition_changes
		// 3. revealing changed definitions
		"SELECT unit, `level` \n\
		FROM unit_authors "+db.forceIndex('unitAuthorsIndexByAddressDefinitionChash')+" \n\
		CROSS JOIN units USING(unit) \n\
		WHERE address IN(?) AND definition_chash=address AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
		UNION \n\
		SELECT unit, `level` \n\
		FROM address_definition_changes \n\
		CROSS JOIN units USING(unit) \n\
		WHERE address_definition_changes.address IN(?) AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
		UNION \n\
		SELECT units.unit, `level` \n\
		FROM address_definition_changes \n\
		CROSS JOIN unit_authors USING(address, definition_chash) \n\
		CROSS JOIN units ON unit_authors.unit=units.unit \n\
		WHERE address_definition_changes.address IN(?) AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
		ORDER BY `level`",
		[witnessAddresses, witnessAddresses, witnessAddresses],
		callback
	)
}

function isUnitStable(unit, callback) {
	db.query("SELECT 1 FROM units WHERE unit=? AND is_stable=1",
	[unit],
	function(rows){
		callback(rows.length > 0);
	})
}

// ################
// validation.js
// ################

function isUnitStored(unit, callback) {
	conn.query(
		"SELECT 1 FROM units WHERE unit=?",
		[unit],
		function(rows){
			callback(rows.length > 0);
		}
	)
}

function findUnitFromBallInHashTreeBalls(ball, callback) {
	db.query(
		"SELECT unit FROM hash_tree_balls WHERE ball=?",
		[ball],
		callback
	)
}

function findBallsInHashTreeAndInBallsFromUnits(units, callback) {
	db.query(
		"SELECT ball FROM hash_tree_balls WHERE unit IN(?) \n\
		UNION \n\
		SELECT ball FROM balls WHERE unit IN(?) \n\
		ORDER BY ball",
		[units, units],
		callback
	)
}

function getUnit(unit, callback) {
	db.query(
		"SELECT unit, is_stable, is_on_main_chain, main_chain_index FROM units WHERE unit=?",
		[unit],
		callback
	)
}

function getUnitFull(unit, includeBall, callback) {
	var join = includeBall ? 'LEFT JOIN balls USING(unit) LEFT JOIN hash_tree_balls ON units.unit=hash_tree_balls.unit' : '';
	var field = includeBall ? ', IFNULL(balls.ball, hash_tree_balls.ball) AS ball' : '';
	db.query(
		"SELECT units.*"+field+" FROM units "+join+" WHERE units.unit=?",
		unit,
		callback
	)
}

function findDuplicateAddressesForUnits(units, callback) {
	db.query(
		"SELECT address, COUNT(*) AS c \n\
		FROM unit_authors WHERE unit IN(?) \n\
		GROUP BY address HAVING c>1",
		[units],
		callback)
}

function findMaxLastBallMciAmoungsUnits(units, callback) {
	db.query(
		"SELECT MAX(lb_units.main_chain_index) AS max_parent_last_ball_mci \n\
		FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \n\
		WHERE units.unit IN(?)",
		[units],
		callback
	)
}

function findErrorFromUnits(units, callback) {
	db.query(
		"SELECT error FROM known_bad_joints WHERE unit IN(?)",
		[units],
		callback
	)

}

function getUnitWithBallAndMaxMci(unit, callback) {
	db.query(
		"SELECT is_stable, is_on_main_chain, main_chain_index, ball, (SELECT MAX(main_chain_index) FROM units) AS max_known_mci \n\
		FROM units LEFT JOIN balls USING(unit) WHERE unit=?",
		[unit],
		callback
	)
}

function findBallFromUnit(unit, callback) {
	db.query(
		"SELECT ball FROM balls WHERE unit=?",
		[unit],
		callback
	)

}

function isAnyReferenceInWitnessAddressDefinitions(witnesses, lastBallMci, callback) {

	var cross = (conf.storage === 'sqlite') ? 'CROSS' : ''; // correct the query planner
	conn.query(
		"SELECT 1 \n\
		FROM address_definition_changes \n\
		JOIN definitions USING(definition_chash) \n\
		JOIN units AS change_units USING(unit)   -- units where the change was declared \n\
		JOIN unit_authors USING(definition_chash) \n\
		JOIN units AS definition_units ON unit_authors.unit=definition_units.unit   -- units where the definition was disclosed \n\
		WHERE address_definition_changes.address IN(?) AND has_references=1 \n\
			AND change_units.is_stable=1 AND change_units.main_chain_index<=? AND +change_units.sequence='good' \n\
			AND definition_units.is_stable=1 AND definition_units.main_chain_index<=? AND +definition_units.sequence='good' \n\
		UNION \n\
		SELECT 1 \n\
		FROM definitions \n\
		"+cross+" JOIN unit_authors USING(definition_chash) \n\
		JOIN units AS definition_units ON unit_authors.unit=definition_units.unit   -- units where the definition was disclosed \n\
		WHERE definition_chash IN(?) AND has_references=1 \n\
			AND definition_units.is_stable=1 AND definition_units.main_chain_index<=? AND +definition_units.sequence='good' \n\
		LIMIT 1",
		[witnesses, lastBallMci, lastBallMci, witnesses, lastBallMci],
		function(rows) {
			callback(rows.length == 0)
		}
	)
}

function getUnitWithSequence(unit, callback) {
	db.query(
		"SELECT unit, sequence, is_stable, is_on_main_chain, main_chain_index FROM units WHERE unit=?",
		[unit],
		callback
	)
}

function countStableGoodWitnesses(witnesses, lastBallMci, callback) {
	// check that all witnesses are already known and their units are good and stable
	db.query(
		// address=definition_chash is true in the first appearence of the address
		// (not just in first appearence: it can return to its initial definition_chash sometime later)
		"SELECT COUNT(DISTINCT address) AS count_stable_good_witnesses FROM unit_authors JOIN units USING(unit) \n\
		WHERE address=definition_chash AND +sequence='good' AND is_stable=1 AND main_chain_index<=? AND address IN(?)",
		[lastBallMci, witnesses],
		callback
	)
}


function findOtherUnitsFromAddress(address, unit, minMci) {
	conn.query( // _left_ join forces use of indexes in units
		/*	"SELECT unit, is_stable \n\
			FROM units \n\
			"+cross+" JOIN unit_authors USING(unit) \n\
			WHERE address=? AND (main_chain_index>? OR main_chain_index IS NULL) AND unit != ?",
			[objAuthor.address, objValidationState.max_parent_limci, objUnit.unit],*/
		"SELECT unit, is_stable \n\
		FROM unit_authors \n\
		CROSS JOIN units USING(unit) \n\
		WHERE address=? AND _mci>? AND unit != ? \n\
		UNION \n\
		SELECT unit, is_stable \n\
		FROM unit_authors \n\
		CROSS JOIN units USING(unit) \n\
		WHERE address=? AND _mci IS NULL AND unit != ?",
		[objAuthor.address, objValidationState.max_parent_limci, objUnit.unit, objAuthor.address, objUnit.unit],
		callback
	)
}


function findUnitsWithAddressDefinitionChanges(address, minMci, callback) {
	db.query(
		"SELECT unit FROM address_definition_changes JOIN units USING(unit) \n\
		WHERE address=? AND (is_stable=0 OR main_chain_index>? OR main_chain_index IS NULL)",
		[address, minMci],
		callback
	)
}

function findUnitsWithPendinginDefinitions(address, minMci) {
	//var filter = bNonserial ? "AND sequence='good'" : "";
	//	var cross = (objValidationState.max_known_mci - objValidationState.last_ball_mci < 1000) ? 'CROSS' : '';
	db.query( // _left_ join forces use of indexes in units
		//	"SELECT unit FROM units "+cross+" JOIN unit_authors USING(unit) \n\
		//	WHERE address=? AND definition_chash IS NOT NULL AND ( /* is_stable=0 OR */ main_chain_index>? OR main_chain_index IS NULL)",
		//	[objAuthor.address, objValidationState.last_ball_mci],
			"SELECT unit FROM unit_authors WHERE address=? AND definition_chash IS NOT NULL AND _mci>?  \n\
			UNION \n\
			SELECT unit FROM unit_authors WHERE address=? AND definition_chash IS NOT NULL AND _mci IS NULL",
			[address, minMci, address],
			callback
	)
}

function findSpendProofsForUnit(unit, spendProofs, callback) {
	var arrEqs = spendProofs.map(function(objSpendProof){
		return "spend_proof="+conn.escape(objSpendProof.spend_proof)+
			" AND address="+conn.escape(objSpendProof.address ? objSpendProof.address : objUnit.authors[0].address);
	});
	db.query(
		"SELECT address, unit, main_chain_index, sequence \n\
		FROM spend_proofs JOIN units USING(unit) \n\
		WHERE unit != ? AND ("+arrEqs.join(" OR ")+")",
		[unit],
		callback
	)
}

function findPollFromUnitAndChoice(unit, choice, callback) {
	db.query(
		"SELECT main_chain_index, sequence FROM polls JOIN poll_choices USING(unit) JOIN units USING(unit) WHERE unit=? AND choice=?",
		[unit, choice],
		callback
	)
}

function getAsset(asset, denomination, callback){
	db.query(
		"SELECT count_coins FROM asset_denominations WHERE asset=? AND denomination=?",
		[asset, denomination],
		callback
	)
}

function findOutput(unit, messageIndex, outputIndex, callback) {
	db.query(
		"SELECT amount, is_stable, sequence, address, main_chain_index, denomination, asset \n\
		FROM outputs \n\
		JOIN units USING(unit) \n\
		WHERE outputs.unit=? AND message_index=? AND output_index=?",
		[unit, messageIndex, outputIndex],
		callback
	)
}

function findPrivatePayment(unit, messageIndex, callback) {
	db.query(
		"SELECT payload_hash, app, units.sequence, units.is_stable, lb_units.main_chain_index AS last_ball_mci \n\
		FROM messages JOIN units USING(unit) \n\
		LEFT JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \n\
		WHERE messages.unit=? AND message_index=?",
		[unit, messageIndex],
		callback
	)
}

function findUnitParents(unit, callback) {
	db.query(
		"SELECT parent_unit FROM parenthoods WHERE child_unit=? ORDER BY parent_unit",
		[unit],
		callback
	)
}

// ################
// graph.js
// ################

function getUnitsToCompare(units, callback) {
	db.query(
		"SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain, is_free FROM units WHERE unit IN(?)",
		[units],
		callback
	)
}

function findUnitParentsFull(units, callback) {
	db.query(
		"SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain \n\
		FROM parenthoods JOIN units ON parent_unit=unit \n\
		WHERE child_unit IN(?)",
		[units],
		callback
	);
}

function findUnitChildrenFull(units, callback) {
	db.query(
		"SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain \n\
		FROM parenthoods JOIN units ON child_unit=unit \n\
		WHERE parent_unit IN(?)",
		[units],
		callback
	)
}

function findUnitChildrenByAuthorsBeforeMci(units, addresses, mci, lastestIncludedMci, callback) {
	db.query(
		"SELECT units.unit, unit_authors.address AS author_in_list \n\
		FROM parenthoods \n\
		JOIN units ON child_unit=units.unit \n\
		LEFT JOIN unit_authors ON unit_authors.unit=units.unit AND address IN(?) \n\
		WHERE parent_unit IN(?) AND latest_included_mc_index<? AND main_chain_index<=?",
		[addresses, units, lastestIncludedMci, mci],
		callback
	)
}

function findUnitsForAddresses(addresses, minMci, maxMci) {
	db.query( // _left_ join forces use of indexes in units
		"SELECT unit FROM units "+db.forceIndex("byMcIndex")+" LEFT JOIN unit_authors USING(unit) \n\
		WHERE latest_included_mc_index>=? AND main_chain_index>? AND main_chain_index<=? AND latest_included_mc_index<? AND address IN(?)",
		[minMci, minMci, maxMci, maxMci, addresses],
//        "SELECT unit FROM units WHERE latest_included_mc_index>=? AND main_chain_index<=?",
//        [objEarlierUnitProps.main_chain_index, to_main_chain_index],
		callback
	)
}

function findUnitChildrenBeforeMciAndLevel(units, mci, level){
	db.query(
		"SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain \n\
		FROM parenthoods JOIN units ON child_unit=unit \n\
		WHERE parent_unit IN(?) AND latest_included_mc_index<? AND level<=?",
		[units, mci, level],
		callback
	)
}

function findUnitParentsAfterMciWithLevel(units, mci, level, callback){
	db.query(
		"SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain \n\
		FROM parenthoods JOIN units ON parent_unit=unit \n\
		WHERE child_unit IN(?) AND (main_chain_index>? OR main_chain_index IS NULL) AND level>=?",
		[units, mci, level],
		callback
	)
}

// ####################
// headers_commision.js
// ####################

function createHeadersCommissionContributions(sinceMcIndex, cb) {

	// there is no SHA1 in sqlite, have to do it in js
	conn.cquery(
		// chunits is any child unit and contender for headers commission, punits is hc-payer unit
		"SELECT chunits.unit AS child_unit, punits.headers_commission, next_mc_units.unit AS next_mc_unit, punits.unit AS payer_unit \n\
			FROM units AS chunits \n\
			JOIN parenthoods ON chunits.unit=parenthoods.child_unit \n\
			JOIN units AS punits ON parenthoods.parent_unit=punits.unit \n\
			JOIN units AS next_mc_units ON next_mc_units.is_on_main_chain=1 AND next_mc_units.main_chain_index=punits.main_chain_index+1 \n\
			WHERE chunits.is_stable=1 \n\
				AND +chunits.sequence='good' \n\
				AND punits.main_chain_index>? \n\
				AND +punits.sequence='good' \n\
				AND punits.is_stable=1 \n\
				AND chunits.main_chain_index-punits.main_chain_index<=1 \n\
				AND next_mc_units.is_stable=1",
		[sinceMcIndex],
		function (rows) {
			// in-memory
			var assocChildrenInfosRAM = {};
			var arrParentUnits = storage.assocStableUnitsByMci[sinceMcIndex + 1].filter(function (props) {
				return props.sequence === 'good'
			});
			arrParentUnits.forEach(function (parent) {
				if (!assocChildrenInfosRAM[parent.unit]) {
					if (!storage.assocStableUnitsByMci[parent.main_chain_index + 1]) { // hack for genesis unit where we lose hc
						if (sinceMcIndex == 0)
							return;
						throwError("no storage.assocStableUnitsByMci[parent.main_chain_index+1] on " + parent.unit);
					}
					var next_mc_unit_props = storage.assocStableUnitsByMci[parent.main_chain_index + 1].find(function (props) {
						return props.is_on_main_chain
					});
					if (!next_mc_unit_props) {
						throwError("no next_mc_unit found for unit " + parent.unit);
					}
					var next_mc_unit = next_mc_unit_props.unit;
					var filter_func = function (child) {
						return (child.sequence === 'good' && child.parent_units && child.parent_units.indexOf(parent.unit) > -1);
					};
					var arrSameMciChildren = storage.assocStableUnitsByMci[parent.main_chain_index].filter(filter_func);
					var arrNextMciChildren = storage.assocStableUnitsByMci[parent.main_chain_index + 1].filter(filter_func);
					var arrCandidateChildren = arrSameMciChildren.concat(arrNextMciChildren);
					var children = arrCandidateChildren.map(function (child) {
						return {
							child_unit: child.unit,
							next_mc_unit: next_mc_unit
						};
					});
					//	var children = _.map(_.pickBy(storage.assocStableUnits, function(v, k){return (v.main_chain_index - props.main_chain_index == 1 || v.main_chain_index - props.main_chain_index == 0) && v.parent_units.indexOf(props.unit) > -1 && v.sequence === 'good';}), function(props, unit){return {child_unit: unit, next_mc_unit: next_mc_unit}});
					assocChildrenInfosRAM[parent.unit] = {
						headers_commission: parent.headers_commission,
						children: children
					};
				}
			});
			var assocChildrenInfos = conf.bFaster ? assocChildrenInfosRAM : {};
			// sql result
			if (!conf.bFaster) {
				rows.forEach(function (row) {
					var payer_unit = row.payer_unit;
					var child_unit = row.child_unit;
					if (!assocChildrenInfos[payer_unit])
						assocChildrenInfos[payer_unit] = {
							headers_commission: row.headers_commission,
							children: []
						};
					else if (assocChildrenInfos[payer_unit].headers_commission !== row.headers_commission)
						throw Error("different headers_commission");
					delete row.headers_commission;
					delete row.payer_unit;
					assocChildrenInfos[payer_unit].children.push(row);
				});
				if (!_.isEqual(assocChildrenInfos, assocChildrenInfosRAM)) {
					// try sort children
					var assocChildrenInfos2 = _.cloneDeep(assocChildrenInfos);
					_.forOwn(assocChildrenInfos2, function (props, unit) {
						props.children = _.sortBy(props.children, ['child_unit']);
					});
					_.forOwn(assocChildrenInfosRAM, function (props, unit) {
						props.children = _.sortBy(props.children, ['child_unit']);
					});
					if (!_.isEqual(assocChildrenInfos2, assocChildrenInfosRAM))
						throwError("different assocChildrenInfos, db: " + JSON.stringify(assocChildrenInfos) + ", ram: " + JSON.stringify(assocChildrenInfosRAM));
				}
			}

			var assocWonAmounts = {}; // amounts won, indexed by child unit who won the hc, and payer unit
			for (var payer_unit in assocChildrenInfos) {
				var headers_commission = assocChildrenInfos[payer_unit].headers_commission;
				var winnerChildInfo = getWinnerInfo(assocChildrenInfos[payer_unit].children);
				var child_unit = winnerChildInfo.child_unit;
				if (!assocWonAmounts[child_unit])
					assocWonAmounts[child_unit] = {};
				assocWonAmounts[child_unit][payer_unit] = headers_commission;
			}
			//console.log(assocWonAmounts);
			var arrWinnerUnits = Object.keys(assocWonAmounts);
			if (arrWinnerUnits.length === 0)
				return cb();
			var strWinnerUnitsList = arrWinnerUnits.map(db.escape).join(', ');
			conn.cquery(
				"SELECT \n\
						unit_authors.unit, \n\
						unit_authors.address, \n\
						100 AS earned_headers_commission_share \n\
					FROM unit_authors \n\
					LEFT JOIN earned_headers_commission_recipients USING(unit) \n\
					WHERE unit_authors.unit IN(" + strWinnerUnitsList + ") AND earned_headers_commission_recipients.unit IS NULL \n\
					UNION ALL \n\
					SELECT \n\
						unit, \n\
						address, \n\
						earned_headers_commission_share \n\
					FROM earned_headers_commission_recipients \n\
					WHERE unit IN(" + strWinnerUnitsList + ")",
				function (profit_distribution_rows) {
					// in-memory
					var arrValuesRAM = [];
					for (var child_unit in assocWonAmounts) {
						var objUnit = storage.assocStableUnits[child_unit];
						for (var payer_unit in assocWonAmounts[child_unit]) {
							var full_amount = assocWonAmounts[child_unit][payer_unit];
							if (objUnit.earned_headers_commission_recipients) { // multiple authors or recipient is another address
								for (var address in objUnit.earned_headers_commission_recipients) {
									var share = objUnit.earned_headers_commission_recipients[address];
									var amount = Math.round(full_amount * share / 100.0);
									arrValuesRAM.push("('" + payer_unit + "', '" + address + "', " + amount + ")");
								};
							} else
								arrValuesRAM.push("('" + payer_unit + "', '" + objUnit.author_addresses[0] + "', " + full_amount + ")");
						}
					}
					// sql result
					var arrValues = conf.bFaster ? arrValuesRAM : [];
					if (!conf.bFaster) {
						profit_distribution_rows.forEach(function (row) {
							var child_unit = row.unit;
							for (var payer_unit in assocWonAmounts[child_unit]) {
								var full_amount = assocWonAmounts[child_unit][payer_unit];
								if (!full_amount)
									throw Error("no amount for child unit " + child_unit + ", payer unit " + payer_unit);
								// note that we round _before_ summing up header commissions won from several parent units
								var amount = (row.earned_headers_commission_share === 100) ?
									full_amount :
									Math.round(full_amount * row.earned_headers_commission_share / 100.0);
								// hc outputs will be indexed by mci of _payer_ unit
								arrValues.push("('" + payer_unit + "', '" + row.address + "', " + amount + ")");
							}
						});
						if (!_.isEqual(arrValuesRAM.sort(), arrValues.sort())) {
							throwError("different arrValues, db: " + JSON.stringify(arrValues) + ", ram: " + JSON.stringify(arrValuesRAM));
						}
					}

					conn.query(
						"INSERT INTO headers_commission_contributions (unit, address, amount) VALUES " + arrValues.join(", "),
						cb
					);
				}
			);
		}
	);
}

function getWinnerInfo(arrChildren){
	if (arrChildren.length === 1)
		return arrChildren[0];
	arrChildren.forEach(function(child){
		child.hash = crypto.createHash("sha1").update(child.child_unit + child.next_mc_unit, "utf8").digest("hex");
	});
	arrChildren.sort(function(a, b){ return ((a.hash < b.hash) ? -1 : 1); });
	return arrChildren[0];
}

function creatHeadersCommissionOutputs(sinceMcIndex, cb) {
	conn.query(
		"INSERT INTO headers_commission_outputs (main_chain_index, address, amount) \n\
		SELECT main_chain_index, address, SUM(amount) FROM headers_commission_contributions JOIN units USING(unit) \n\
		WHERE main_chain_index>? \n\
		GROUP BY main_chain_index, address",
		[sinceMcIndex],
		cb
	)
}

function findMciFromHeadersCommissionContributions(sinceMcIndex, cb){
	conn.query(
		"SELECT DISTINCT main_chain_index FROM headers_commission_contributions JOIN units USING(unit) WHERE main_chain_index>?",
		[sinceMcIndex], 
		cb
	)
}

function findMaxSpendableMciFromHeadersCommissionOutputs(cb){
	conn.query(
		"SELECT MAX(main_chain_index) AS max_spendable_mci FROM headers_commission_outputs",
		cb
	)
}
