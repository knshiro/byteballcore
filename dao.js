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
