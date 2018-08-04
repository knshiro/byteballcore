/*jslint node: true */
"use strict";
var _ = require('lodash');
var constants = require('./constants.js');	
var dao = require('./dao');


function readBalances(wallet, options, handleBalances) {

	var outputsOnly = !!options.outputsOnly;
	var includeEmptyBalances = !!options.includeEmptyBalances; 

	var assocBalances = {base: {stable: 0, pending: 0}};
	assocBalances[constants.BLACKBYTES_ASSET] = {is_private: 1, stable: 0, pending: 0};


	dao.readBalances(
		wallet,
		{ outputsOnly: outputsOnly }, 
		function(rows){
			for (var i=0; i<rows.length; i++){
				var row = rows[i];
				var asset = row.asset || "base";
				if (!assocBalances[asset])
					assocBalances[asset] = {stable: 0, pending: 0};
				assocBalances[asset][row.is_stable ? 'stable' : 'pending'] = row.balance;
			}
			if (includeEmptyBalances) {
				// add 0-balance assets
				dao.listAllAssetsForWallet(
					wallet,
					function (rows) {
						for (var i = 0; i < rows.length; i++) {
							var row = rows[i];
							var asset = row.asset || "base";
							if (!assocBalances[asset])
								assocBalances[asset] = {
									stable: 0,
									pending: 0
								};
							assocBalances[asset].is_private = row.is_private;
						}
						if (assocBalances[constants.BLACKBYTES_ASSET].stable === 0 && assocBalances[constants.BLACKBYTES_ASSET].pending === 0)
								delete assocBalances[constants.BLACKBYTES_ASSET];
						handleBalance(assocBalances);
					}
				);
			} else {
				handleBalance(assocBalances);
			}
		}
	);
}

function readOutputsBalance(wallet, handleBalance){
	readBalances(wallet, {outputsOnly: true, includeEmptyBalances: false}, handleBalance);
}

function readBalance(wallet, handleBalance){
	readBalances(wallet, {outputsOnly: false, includeEmptyBalances: true}, handleBalance);	
}

function readSharedAddressesOnWallet(wallet, handleSharedAddresses){
	dao.readSharedAddressesOnWallet(wallet, function(rows){
		var arrSharedAddresses = rows.map(function(row){ return row.shared_address; });
		if (arrSharedAddresses.length === 0)
			return handleSharedAddresses([]);
		readSharedAddressesDependingOnAddresses(arrSharedAddresses, function(arrNewSharedAddresses){
			handleSharedAddresses(arrSharedAddresses.concat(arrNewSharedAddresses));
		});
	});
}

function readSharedAddressesDependingOnAddresses(arrMemberAddresses, handleSharedAddresses){
	dao.readSharedAddressesDependingOnAddresses(arrMemberAddresses, function(rows){
		var arrSharedAddresses = rows.map(function(row){ return row.shared_address; });
		if (arrSharedAddresses.length === 0)
			return handleSharedAddresses([]);
		var arrNewMemberAddresses = _.difference(arrSharedAddresses, arrMemberAddresses);
		if (arrNewMemberAddresses.length === 0)
			return handleSharedAddresses([]);
		readSharedAddressesDependingOnAddresses(arrNewMemberAddresses, function(arrNewSharedAddresses){
			handleSharedAddresses(arrNewMemberAddresses.concat(arrNewSharedAddresses));
		});
	});
}


function readSharedBalance(wallet, handleBalance){
	var assocBalances = {};
	readSharedAddressesOnWallet(wallet, function(arrSharedAddresses){
		if (arrSharedAddresses.length === 0)
			return handleBalance(assocBalances);
		dao.readBalances(
			arrSharedAddresses,
			{ perAdress: true }, 
			function(rows){
				for (var i=0; i<rows.length; i++){
					var row = rows[i];
					var asset = row.asset || "base";
					if (!assocBalances[asset])
						assocBalances[asset] = {};
					if (!assocBalances[asset][row.address])
						assocBalances[asset][row.address] = {stable: 0, pending: 0};
					assocBalances[asset][row.address][row.is_stable ? 'stable' : 'pending'] += row.balance;
				}
				handleBalance(assocBalances);
			}
		);
	});
}

exports.readBalance = readBalance;
exports.readOutputsBalance = readOutputsBalance;
exports.readSharedBalance = readSharedBalance;