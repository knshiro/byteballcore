/*jslint node: true */
"use strict";
var crypto = require('crypto');
var async = require('async');
var dao = require('./dao.js');
var conf = require('./conf.js');
var _ = require('lodash');
var storage = require('./storage.js');

var max_spendable_mci = null;

function calcHeadersCommissions(conn, onDone){
	// we don't require neither source nor recipient to be majority witnessed -- we don't want to return many times to the same MC index.
	console.log("will calc h-comm");
	if (max_spendable_mci === null) // first calc after restart only
		return initMaxSpendableMci(conn, function(){ calcHeadersCommissions(conn, onDone); });

	// max_spendable_mci is old, it was last updated after previous calc
	var since_mc_index = max_spendable_mci;

	async.series([
		function(cb){
			dao.createHeadersCommissionContributions(since_mc_index, cb)
		},
		function(cb){
			dao.creatHeadersCommissionOutputs(
				since_mc_index,
				function(){
					if (conf.bFaster)
						return cb();
					dao.findMciFromHeadersCommissionContributions(
						since_mc_index,
						function(contrib_rows){
							if (contrib_rows.length === 1 && contrib_rows[0].main_chain_index === since_mc_index+1 || since_mc_index === 0)
								return cb();
							throwError("since_mc_index="+since_mc_index+" but contributions have mcis "+contrib_rows.map(function(r){ return r.main_chain_index}).join(', '));
						}
					);
				}
			);
		},
		function(cb){
			dao.findMaxSpendableMciFromHeadersCommissionOutputs(function(rows){
				max_spendable_mci = rows[0].max_spendable_mci;
				cb();
			});
		}
	], onDone);

}

function initMaxSpendableMci(conn, onDone){
	dao.findMaxSpendableMciFromHeadersCommissionOutputs(function(rows){
		max_spendable_mci = rows[0].max_spendable_mci || 0; // should be -1, we lose headers commissions paid by genesis unit
		if (onDone)
			onDone();
	});
}

function resetMaxSpendableMci(){
	max_spendable_mci = null;
}

function getMaxSpendableMciForLastBallMci(last_ball_mci){
	return last_ball_mci - 1;
}

function throwError(msg){
	var eventBus = require('./event_bus.js');
	debugger;
	if (typeof window === 'undefined')
		throw Error(msg);
	else
		eventBus.emit('nonfatal_error', msg, new Error());
}

exports.resetMaxSpendableMci = resetMaxSpendableMci;
exports.calcHeadersCommissions = calcHeadersCommissions;
exports.getMaxSpendableMciForLastBallMci = getMaxSpendableMciForLastBallMci;

