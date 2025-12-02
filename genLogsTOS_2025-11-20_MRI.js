const fs = require('fs');
const path = require("path");


// const SPX_HOME = process.env.SPX_HOME
// const SPX_HOME = "C:\MEIC\log\"
const SPX_HOME = "C:\\MEIC\\log";


let DATE
let OUTPUT_TYPE
let SELECTION
let SPX_CLOSE

let transactions = []
let leftOverTransactions = []
let ics = []
let earlyOrders = []
let itms = []

const usCurrency = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
})

function monitor () {

    getOpts()

    getOrders()

    getTransactions()

    assignEarlies()

    // dumpData()

    matchTransactions()

    finalizeOrders()

    processITMs()

    if (OUTPUT_TYPE === 'CSV') {
        exportCSV()
    } else {
        printResults()
    }

}


monitor()

function getOrders() {

    // get all orders
    // const ordersFile = SPX_HOME + '/' + DATE + '/transactions/orders-' + DATE + '.json'

    const ordersFile = SPX_HOME + "\\" + DATE + "\\" + "orders.json"
    console.log("updated ordersFile:<" + ordersFile + ">")

    try {
        const fileData = fs.readFileSync(ordersFile, 'utf8')
        const data = JSON.parse(fileData)
        data.forEach((_o) => {
            console.log("displaying _o:" + _o)
            processOrder(_o)
        })
        console.log("orders file processing successful")

    } catch (err) {
        console.log("No Orders")
    }

}

function getTransactions() {

    // get all transactions
    let tc = 0
    // const transFile = SPX_HOME + '/' + DATE + '/transactions/transactions-' + DATE + '.json'

    const transFile = SPX_HOME + "\\" + DATE + "\\" + "transactions.json"
    console.log("updated transFile:<" + transFile + ">")





    try {
        const fileData = fs.readFileSync(transFile, 'utf8')
        const data = JSON.parse(fileData)
        data.forEach((_t) => {
            tc++
            processTransaction(_t)
        })
    } catch (err) {
        console.log("No Transactions")
    }
    // console.log('tc = ' + tc)

}

function processITMs() {

    // get SPX closing
    const spxFile = SPX_HOME + '/' + DATE + '/transactions/spx-' + DATE + '.json'
    try {
        const fileData = fs.readFileSync(spxFile, 'utf8')
        let data = undefined
        if (fileData) {
            data = JSON.parse(fileData)
        } else {
            data = undefined
        }
        if (!data || data.empty || !data.candles || !data.candles[0] || !data.candles[0].close) {
            SPX_CLOSE = undefined
        } else {
            SPX_CLOSE = data.candles[0].close
        }

    } catch (err) {
        console.log("No SPX CLOSE data")
    }

     if (SPX_CLOSE) {

        ics.forEach((ic) => {

            if (SPX_CLOSE < ic.putSpread.spread.short && ic.putSpread.filledShortBTC === false) {
                 let itm = {
                    putCall: 'PUT ',
                    longShort: 'SHORT',
                    symbol: ic.putSpread.shortSymbol,
                    amount: Number(SPX_CLOSE - ic.putSpread.spread.short) * 100,
                    spx_close: SPX_CLOSE
                }
                itms.push(itm)
            }
            if (SPX_CLOSE > ic.callSpread.spread.short && ic.callSpread.filledShortBTC === false) {
                let itm = {
                    putCall: 'CALL',
                    longShort: 'SHORT',
                    symbol: ic.callSpread.shortSymbol,
                    amount: Number(ic.callSpread.spread.short - SPX_CLOSE) * 100,
                    spx_close: SPX_CLOSE
                }
                itms.push(itm)
            }
            if (SPX_CLOSE < ic.putSpread.spread.long && ic.putSpread.filledLongSTC === false) {
                let itm = {
                    putCall: 'PUT ',
                    longShort: 'LONG',
                    symbol: ic.putSpread.longSymbol,
                    amount: Number(ic.putSpread.spread.long - SPX_CLOSE) * 100,
                    spx_close: SPX_CLOSE
                }
                itms.push(itm)
            }
            if (SPX_CLOSE > ic.callSpread.spread.long && ic.callSpread.filledLongSTC === false) {
                let itm = {
                    putCall: 'CALL',
                    longShort: 'LONG',
                    symbol: ic.callSpread.longSymbol,
                    amount: Number(SPX_CLOSE - ic.callSpread.spread.long) * 100,
                    spx_close: SPX_CLOSE
                }
                itms.push(itm)
            }
        })
    }


}


function getOpts() {
    const args = process.argv.slice(2);

    if (args[0]) {
        DATE = args[0];
        console.log('Provided Date: ' + DATE);
    } else {
        const now = new Date();
        const yyyy = now.getFullYear();
        const mm = String(now.getMonth() + 1).padStart(2, '0');
        const dd = String(now.getDate()).padStart(2, '0');
        DATE = `${yyyy}-${mm}-${dd}`;
        console.log('Defaulting to current Date: ' + DATE);
    }

    OUTPUT_TYPE = 'TEXT';
    if (args[1]) {
        if (args[1] === 'CSV') {
            OUTPUT_TYPE = 'CSV';
        }
    }

    SELECTION = 'ALL';
    if (args[2]) {
        if (args[2] === 'ALL') {
            SELECTION = 'ALL';
        }
        if (args[2] === 'ICS') {
            SELECTION = 'ICS';
        }
        if (args[2] === 'SUMMARY') {
            SELECTION = 'SUMMARY';
        }
        if (args[2] === 'SPREADS') {
            SELECTION = 'SPREADS';
        }
    }
}

function dumpData() {

    console.log('\nTransactions:')
    transactions.forEach((_t) => {
        console.log(_t.orderId + ',' + _t.time + ',' + _t.putCall + ',' + _t.strike + ',' + _t.qty + ',' + Number(_t.netAmount).toFixed(2) + ',' + _t.position)
    })

    console.log('\nICs:')
    ics.forEach((_o) => {
        console.log(JSON.stringify(_o, null, 2))
    })

    console.log('\nEarlyOrders:')
    earlyOrders.forEach((_eo) => {
        console.log(JSON.stringify(_eo, null, 2))
    })

}


function assignEarlies() {

    earlyOrders.forEach((eo) => {

        let found = false
        ics.forEach((ic) => {

            let temp_eo = eo
            let temp_ic = ic
            if (!found && temp_eo.putCall === 'PUT' && temp_eo.instruction === 'BUY_TO_CLOSE') {
                if (!hasOrderId(temp_ic.putSpread.shortStopOrderId) && isAfter(temp_eo.time, temp_ic.time)) {
                    if (temp_eo.symbol === temp_ic.putSpread.shortSymbol) {
                        // console.log (JSON.stringify(temp_ic, null, 2))
                        found = true
                        temp_ic.putSpread.shortStopOrderId = temp_eo.orderId
                        temp_ic.putSpread.isEarly = true
                    }
                }
            }
            if (!found && temp_eo.putCall === 'CALL' && temp_eo.instruction === 'BUY_TO_CLOSE') {
                if (!hasOrderId(temp_ic.callSpread.shortStopOrderId) && isAfter(temp_eo.time, temp_ic.time)) {
                    if (temp_eo.symbol === temp_ic.callSpread.shortSymbol) {
                        found = true
                        temp_ic.callSpread.shortStopOrderId = temp_eo.orderId
                        temp_ic.callSpread.isEarly = true
                    }
                }
            }
            if (!found && temp_eo.putCall === 'PUT' && temp_eo.instruction === 'SELL_TO_CLOSE') {
                if (!hasOrderId(temp_ic.putSpread.longStopOrderId) && isAfter(temp_eo.time, temp_ic.time)) {
                    if (temp_eo.symbol === temp_ic.putSpread.longSymbol) {
                        found = true
                        temp_ic.putSpread.longStopOrderId = temp_eo.orderId
                    }
                }
            }
            if (!found && temp_eo.putCall === 'CALL' && temp_eo.instruction === 'SELL_TO_CLOSE') {
                if (!hasOrderId(temp_ic.callSpread.longStopOrderId) && isAfter(temp_eo.time, temp_ic.time)) {
                    if (temp_eo.symbol === temp_ic.callSpread.longSymbol) {
                        found = true
                        temp_ic.callSpread.longStopOrderId = temp_eo.orderId
                    }
                }
            }
        })
    })
}

function hasOrderId(orderId) {
    if (orderId === undefined || orderId === -1) {
        return false
    }
    return true
}

function finalizeOrders() {

    ics.forEach((ic) => {

        ic.putSpread.spreadNetCredit = Number((ic.putSpread.shortSTO - ic.putSpread.longBTO).toFixed(2))
        if (!ic.putSpread.shortBTC) {
            ic.putSpread.shortBTC = 0
            ic.putSpread.shortBTCFees = 0
        }
        if (!ic.putSpread.longSTC) {
            ic.putSpread.longSTC = 0
            ic.putSpread.longSTCFees = 0
        }
        let pl = Number(ic.putSpread.shortSTO * 100)
            - Number(ic.putSpread.shortSTOFees)
            - Number(ic.putSpread.longBTO * 100)
            - Number(ic.putSpread.longBTOFees)
            - Number(ic.putSpread.shortBTC * 100)
            - Number(ic.putSpread.shortBTCFees)
            + Number(ic.putSpread.longSTC * 100)
            - Number(ic.putSpread.longSTCFees)
        pl = Number(pl.toFixed(2))
        ic.putSpread.pl = pl
        if (!ic.putSpread.shortStopped) {
            ic.putSpread.stoppedTime = undefined
        }
        if (!ic.putSpread.isEarly) {
            ic.putSpread.earlyTime = undefined
        }

        ic.callSpread.spreadNetCredit = Number((ic.callSpread.shortSTO - ic.callSpread.longBTO).toFixed(2))
        if (!ic.callSpread.shortBTC) {
            ic.callSpread.shortBTC = 0
            ic.callSpread.shortBTCFees = 0
        }
        if (!ic.callSpread.longSTC) {
            ic.callSpread.longSTC = 0
            ic.callSpread.longSTCFees = 0
        }
        pl = Number(ic.callSpread.shortSTO * 100)
            - Number(ic.callSpread.shortSTOFees)
            - Number(ic.callSpread.longBTO * 100)
            - Number(ic.callSpread.longBTOFees)
            - Number(ic.callSpread.shortBTC * 100)
            - Number(ic.callSpread.shortBTCFees)
            + Number(ic.callSpread.longSTC * 100)
            - Number(ic.callSpread.longSTCFees)
        pl = Number(pl.toFixed(2))
        ic.callSpread.pl = pl
        if (!ic.callSpread.shortStopped) {
            ic.callSpread.stoppedTime = undefined
        }
        if (!ic.callSpread.isEarly) {
            ic.callSpread.earlyTime = undefined
        }


        ic.pl = Number((ic.putSpread.pl + ic.callSpread.pl).toFixed(2))
        ic.netCredit = Number((ic.putSpread.spreadNetCredit + ic.callSpread.spreadNetCredit).toFixed(2))
        ic.stopRisk = ic.em ? (Number(ic.em)  / Number(ic.netCredit)).toFixed(1) : undefined

        if (ic.putSpread.shortStopped && ic.callSpread.shortStopped) {
            ic.status = 'LOSER'
        } else if (
            (ic.putSpread.shortStopped && !ic.callSpread.shortStopped) ||
            (!ic.putSpread.shortStopped && ic.callSpread.shortStopped)
        ) {
            ic.status = 'BE'
        } else if (ic.putSpread.isEarly || ic.callSpread.isEarly) {
            ic.status = 'EARLY'
        } else {
            ic.status = 'WINNER'
        }

    })

}

function getRecommendationInfoAtTime(time) {

    let recommendationFileName = getRecommendationForOrder(time)
    let recommendation
    let recommendationInfo = {
        putSpreadLimit: undefined,
        callSpreadLimit: undefined,
        underlying: {
            spxLast: undefined,
            em: undefined
        }
    }
    try {
        const recommendationData = fs.readFileSync(recommendationFileName, 'utf8')
        recommendation = JSON.parse(recommendationData)
    } catch (err) {
        // console.log("No Recommendation")
        return recommendationInfo
    }
    if (!recommendation) {
        // console.log("No Recommendation")
        return recommendationInfo
    }
    recommendationInfo = {
        putSpreadLimit: Number(Number(recommendation.ic.putLimit).toFixed(2)),
        callSpreadLimit: Number(Number(recommendation.ic.callLimit).toFixed(2)),
        underlying: {
            spxLast: Number(Number(recommendation.underlying.last).toFixed(0)),
            em: Number(Number(recommendation.underlying.em).toFixed(1))
        }
    }
    return recommendationInfo

}

function hasSubsequentOrder(orderStatus) {

    // orderStatus: FILLED, EXPIRED, REJECTED, CANCELED, AWAITING_PARENT_ORDER, REPLACED
    if (orderStatus === 'CANCELED' || orderStatus === 'REPLACED') {
        return true
    }
    return false
}



function processOrder(order) {

    const o = order

    // adjust the time
    let adjustedDate = fixTime(o.enteredTime)
    let time = convertTimeTime(adjustedDate)

    let recommendationInfo = getRecommendationInfoAtTime(time)

    // We found an IC, process it and add it to the ICs, account for mor than one contract
    if (o.orderType === 'NET_CREDIT' && o.complexOrderStrategyType === 'IRON_CONDOR' && o.status === 'FILLED' && o.orderStrategyType === 'TRIGGER') {

        if (!o.childOrderStrategies || !o.childOrderStrategies[0] || !o.childOrderStrategies[1]) {
            console.error("no spreads in the IC order")
            return
        }

        for (let q = 0; q < o.quantity; q++) {

            let putSpread = {
                spread: {
                    putCall: 'PUT',
                    short: 0,
                    long: 0
                },
                shortStopOrderId: -1,
                longStopOrderId: -1,
                pl: 0,
                shortStopped: false,
                stopPrice: 0,
                shortBTC: 0,
                longSTC: 0,
                shortBTCFees: 0,
                longSTCFees: 0,
                filledShortBTC: false,
                filledLongSTC: false,
                stoppedTime: 'STOPPED TIME',
                earlyTime: 'EARLY TIME',
                status: 'EXPIRED',
                isEarly: false,
                limit: recommendationInfo.putSpreadLimit,
                shortSTO: 0,
                longBTO: 0,
                shortSTOFees: 0,
                longBTOFees: 0,
                filledShortSTO: false,
                filledLongBTO: false,
                spreadNetCredit: 0,
                shortSymbol: undefined,
                longSymbol: undefined
                // ,
                // quantity: 0
            }

            let callSpread = {
                spread: {
                    putCall: 'CALL',
                    short: 0,
                    long: 0
                },
                shortStopOrderId: -1,
                longStopOrderId: -1,
                pl: 0,
                shortStopped: false,
                stopPrice: 0,
                shortBTC: 0,
                longSTC: 0,
                shortBTCFees: 0,
                longSTCFees: 0,
                filledShortBTC: false,
                filledLongSTC: false,
                stoppedTime: 'STOPPED TIME',
                earlyTime: 'EARLY TIME',
                status: 'EXPIRED',
                isEarly: false,
                limit: recommendationInfo.callSpreadLimit,
                shortSTO: 0,
                longBTO: 0,
                shortSTOFees: 0,
                longBTOFees: 0,
                filledShortSTO: false,
                filledLongBTO: false,
                spreadNetCredit: 0,
                shortSymbol: undefined,
                longSymbol: undefined
                // ,
                // quantity: 0
            }

            let ic = {
                orderId: o.orderId,
                putSpread: putSpread,
                callSpread: callSpread,
                time: time,
                pl: 0,
                spxLast: recommendationInfo.underlying.spxLast ? Number(recommendationInfo.underlying.spxLast).toFixed(0) : undefined,
                em: recommendationInfo.underlying.em ? Number(recommendationInfo.underlying.em).toFixed(1) : undefined,
                stopRisk: undefined,
                netCredit: 0,
                limit: o.price,
                status: "WINNER"
            }

            o.orderLegCollection.forEach((leg) => {

                let legId = leg.legId
                let putCall = leg.instrument.putCall
                let instruction = leg.instruction
                let effect = leg.positionEffect
                let orderType = leg.orderType
                // orderStatus = FILLED, EXPIRED, REJECTED, CANCELED, AWAITING_PARENT_ORDER, REPLACED, WORKINGG
                let orderStatus = leg.status

                let option

                if (putCall === 'PUT' && instruction === 'SELL_TO_OPEN') {
                    option = symbolToOption(leg.instrument.symbol)
                    ic.putSpread.spread.short = option.strike
                    // ic.putSpread.shortLeqId = legId
                    ic.putSpread.shortSymbol = leg.instrument.symbol
                } else if (putCall === 'PUT' && instruction === 'BUY_TO_OPEN') {
                    option = symbolToOption(leg.instrument.symbol)
                    ic.putSpread.spread.long = option.strike
                    // ic.putSpread.longLeqId = legId
                    ic.putSpread.longSymbol = leg.instrument.symbol
                } else if (putCall === 'CALL' && instruction === 'SELL_TO_OPEN') {
                    option = symbolToOption(leg.instrument.symbol)
                    ic.callSpread.spread.short = option.strike
                    // ic.callSpread.shortLeqId = legId
                    ic.callSpread.shortSymbol = leg.instrument.symbol
                } else if (putCall === 'CALL' && instruction === 'BUY_TO_OPEN') {
                    option = symbolToOption(leg.instrument.symbol)
                    ic.callSpread.spread.long = option.strike
                    // ic.callSpread.longLeqId = legId
                    ic.callSpread.longSymbol = leg.instrument.symbol
                }

            })

            o.childOrderStrategies.forEach((child) => {

                let childShortStop = child.orderLegCollection[0]
                let childOrderStatus = child.status
                let childChild = child.childOrderStrategies[0]

                if (childShortStop.instrument.symbol === ic.putSpread.shortSymbol && !hasSubsequentOrder(childOrderStatus)) {
                    ic.putSpread.shortStopOrderId = child.orderId
                    ic.putSpread.longStopOrderId = childChild.orderId
                    ic.putSpread.stopPrice = child.stopPrice
                    // ic.putSpread.quantity = child.quantity
                }
                if (childShortStop.instrument.symbol === ic.callSpread.shortSymbol && !hasSubsequentOrder(childOrderStatus)) {
                    ic.callSpread.shortStopOrderId = child.orderId
                    ic.callSpread.longStopOrderId = childChild.orderId
                    ic.callSpread.stopPrice = child.stopPrice
                    // ic.callSpread.quantity = child.quantity
                }

            })

            ics.push(ic)

        }

        return
    }


    if ( (o.orderType === 'MARKET' || o.orderType === 'LIMIT') && (o.complexOrderStrategyType === 'CUSTOM' || o.complexOrderStrategyType === 'NONE') && o.status === 'FILLED') {

        o.orderLegCollection.forEach((leg) => {

            for (let l = 0; l < leg.quantity; l++) {
                let earlyOrder = {
                    symbol: leg.instrument.symbol,
                    instruction: leg.instruction,
                    orderId: o.orderId,
                    time: time,
                    putCall: leg.instrument.putCall
                }
                earlyOrders.push(earlyOrder)
            }
        })
    }

    return

}


function processTransaction(transaction) {

    let t = transaction
    if (t.type === 'TRADE') {

        let adjustedTime
        let formattedTime

        let symbol
        let netAmount
        let putCall
        let strike
        let position
        let orderId
        let amount
        let time
        let filledPrice
        let fees

        t.transferItems.forEach((item) => {

            if (item.instrument.assetType === 'OPTION') {

                adjustedTime = fixTime(t.time)
                formattedTime = convertTimeTime(adjustedTime)

                symbol = item.instrument.symbol
                amount = Math.abs(item.amount)
                netAmount = Math.abs(Number(t.netAmount) / amount) / 100
                putCall = item.instrument.putCall
                strike = symbolToStrike(symbol)
                position = item.positionEffect
                orderId = t.orderId
                time = formattedTime
                filledPrice = Number(item.price)
                fees = Math.abs(Math.abs(Number(item.price * 100) * amount) - Math.abs(Number(t.netAmount)))
                fees = fees / amount

            }
        })

        for (let j = 1; j <= amount; j++) {
            let entry = {
                symbol: symbol,
                netAmount: netAmount,
                putCall: putCall,
                strike: strike,
                position: position,
                orderId: orderId,
                time: time,
                filledPrice: filledPrice,
                fees: fees
            }
            transactions.push(entry)
        }
    }
}

function matchTransactions() {

    // make a working deep copy
    let w = structuredClone(transactions)

    // this is our transaction that we will be working with, walk through the list one by one
    let _t

    //*****************************//
    // now do the work of matching up

    let found = false
    while (w.length > 0) {

        // take the first transaction
        _t = w[0]

        // while found is false, try to match that transaction with a leg of a spread of an IC
        // once a match is made, set found to true so that this transaction is not matched
        // with more than one leg
        ics.forEach((_i) => {

            const icOrderId = _i.orderId

            const putShortStopOrderId = _i.putSpread.shortStopOrderId
            const putLongStopOrderId = _i.putSpread.longStopOrderId
            const callShortStopOrderId = _i.callSpread.shortStopOrderId
            const callLongStopOrderId = _i.callSpread.longStopOrderId

            if (_t.orderId === icOrderId && !found) {

                if (_t.putCall === 'PUT' && _t.strike === _i.putSpread.spread.short && !_i.putSpread.filledShortSTO) {
                    _i.putSpread.shortSTO = Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                    _i.putSpread.shortSTOFees = Number(_t.fees)
                    _i.putSpread.filledShortSTO = true
                    found = true
                }
                if (_t.putCall === 'PUT' && _t.strike === _i.putSpread.spread.long  && !_i.putSpread.filledLongBTO) {
                    _i.putSpread.longBTO = Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                    _i.putSpread.longBTOFees = Number(_t.fees)
                    _i.putSpread.filledLongBTO = true
                    found = true
                }
                if (_t.putCall === 'CALL' && _t.strike === _i.callSpread.spread.short && !_i.callSpread.filledShortSTO) {
                    _i.callSpread.shortSTO = Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                    _i.callSpread.shortSTOFees = Number(_t.fees)
                    _i.callSpread.filledShortSTO = true
                    found = true
                }
                if (_t.putCall === 'CALL' && _t.strike === _i.callSpread.spread.long && !_i.callSpread.filledLongBTO) {
                    _i.callSpread.longBTO = Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                    _i.callSpread.longBTOFees = Number(_t.fees)
                    _i.callSpread.filledLongBTO = true
                    found = true
                }
            }

            if (_t.orderId === putShortStopOrderId && !found && !_i.putSpread.filledShortBTC) {
                _i.putSpread.status = _i.putSpread.isEarly ? 'EARLY' : 'STOPPED'
                _i.putSpread.shortBTC =  Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                _i.putSpread.shortBTCFees = Number(_t.fees)
                _i.putSpread.shortStopped = _i.putSpread.isEarly ? false : true
                _i.putSpread.stoppedTime = _t.time
                _i.putSpread.earlyTime = _t.time
                _i.putSpread.filledShortBTC = true
                found = true
            }

            if (_t.orderId === putLongStopOrderId && !found && !_i.putSpread.filledLongSTC) {
                _i.putSpread.longSTC =  Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                _i.putSpread.longSTCFees =  Number(_t.fees).toFixed(2)
                _i.putSpread.filledLongSTC = true
                found = true
            }

            if (_t.orderId === callShortStopOrderId && !found && !_i.callSpread.filledShortBTC) {
                _i.callSpread.status = _i.callSpread.isEarly ? 'EARLY' : 'STOPPED'
                _i.callSpread.shortBTC =  Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                _i.callSpread.shortBTCFees = Number(_t.fees)
                _i.callSpread.shortStopped = _i.callSpread.isEarly ? false : true
                _i.callSpread.stoppedTime = _t.time
                _i.callSpread.earlyTime = _t.time
                _i.callSpread.filledShortBTC = true
                found = true
            }

            if (_t.orderId === callLongStopOrderId && !found && !_i.callSpread.filledLongSTC) {
                _i.callSpread.longSTC =  Number(Number( Math.abs(_t.filledPrice)).toFixed(2))
                _i.callSpread.longSTCFees =  Number(_t.fees).toFixed(2)
                _i.callSpread.filledLongSTC = true
                found = true
            }

        })

        // matched or not, remove this transaction from the list
        w.splice(0, 1)

        // If there was no match, save the transaction away in the leftover transactions list
        // for reporting or debug.  We don't really use or need these leftover transactions
        if (!found) {
            leftOverTransactions.push(_t)
        }

        found = false
    }

}

function fixTime (timeStamp) {

    // this converts to local time since time stamp is in UTC
    const date = new Date(timeStamp)

    const hours = date.getHours()
    const minutes = date.getMinutes()

    let timezone = Intl.DateTimeFormat().resolvedOptions().timeZone
    let startHour = 14
    if (timezone === 'America/Los_Angeles') {
        if (isDaylightSavingTime()) {
            startHour = 7
        } else {
            startHour = 8
        }
    }
    if (timezone === 'America/Denver') {
        if (isDaylightSavingTime()) {
            startHour = 6
        } else {
            startHour = 7
        }
    }
    if (timezone === 'America/Boise') {
        if (isDaylightSavingTime()) {
            startHour = 6
        } else {
            startHour = 7
        }
    }

    // everything before <startHour>:30 is really from the day before
    if (hours < startHour || (hours === startHour && minutes < 30)) {
        date.setHours(23, 59, 59, 0)
        date.setDate(date.getDate() - 1)
    }

    return date
}


function isDaylightSavingTime() {
    const now = new Date();
    const janOffset = new Date(now.getFullYear(), 0, 1).getTimezoneOffset();  // January (Standard Time)
    const julOffset = new Date(now.getFullYear(), 6, 1).getTimezoneOffset();  // July (DST for most regions)

    return now.getTimezoneOffset() < Math.max(janOffset, julOffset);
}



function convertTimeDate (date) {

//    const date = new Date(timeStamp)

    const mstDate = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Denver",
        hour12: false,
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    }).format(date);

    return mstDate
}

function convertTimeTime (date) {

    const hours = date.getHours().toString().padStart(2, '0')
    const minutes = date.getMinutes().toString().padStart(2, '0')
    const seconds = date.getSeconds().toString().padStart(2, '0')
    const mstDate = `${hours}:${minutes}:${seconds}`

    return mstDate
}

function symbolToStrike (symbol) {
    let matches = symbol.match(/SPXW  2.......([0-9][0-9][0-9][0-9])000/)
    let strike = matches[1]
    return strike
}

function symbolToOption (symbol) {

    // 'SPXW  250411P05295000'
    let matches = symbol.match(/SPXW  2.....([CP]).([0-9][0-9][0-9][0-9])000/)
    let putCall = matches[1] === 'P' ? 'PUT' : 'CALL'
    let strike = matches[2]
    let option = {
        putCall: putCall,
        strike: strike
    }
    return option
}

function OptionToSymbol (option) {

    // 'SPXW  250411P05295000'
    const shortDate = '250417'
    let symbol = 'SPXW  ' + shortDate + (option.putCall === 'PUT' ? 'P' : 'C') + '0' + String(option.strike) + '000'
    return symbol
}



function withinMinutes(minutes, time1, time2) {
    const t1Elements = time1.split(':')
    const t2Elements = time2.split(':')
    const t1Val = (t1Elements[0] * 3600) + (t1Elements[1] * 60) + (t1Elements[2])
    const t2Val = (t2Elements[0] * 3600) + (t2Elements[1] * 60) + (t2Elements[2])
    const diff = Math.abs(t1Val - t2Val)
    if (diff < (minutes * 60)) return true
    return false
}

function isAfter(time1, time2) {
    const t1Elements = time1.split(':')
    const t2Elements = time2.split(':')
    const t1Val = (t1Elements[0] * 3600) + (t1Elements[1] * 60) + (t1Elements[2])
    const t2Val = (t2Elements[0] * 3600) + (t2Elements[1] * 60) + (t2Elements[2])
    if (t1Val >= t2Val) return true
    return false
}


function getSPXAtTime(time) {

    let dataDir = SPX_HOME + '/' + DATE + '/data'

    let spxs = []
    let files = getFilesInDirectory(dataDir)
    let times = []
    files.forEach((fileName) => {
        let time = path.basename(fileName, path.extname(fileName)).replace(/-/g, ':')
        let entry = {
            time: time,
            file: fileName
        }
        times.push(entry)
    })

    let foundTime
    times.forEach((timeObject) => {
        if (compareTimeEarlier(timeObject.time, time)) {

        } else {
            if (!foundTime) {
                foundTime = timeObject
            }
        }
    })

    let spx
    if (foundTime) {
        spx = getSPX(foundTime.file)
    }

    let returnObject = spx
    return returnObject
}

function getFilesInDirectory(dirPath) {
    try {
        // Read directory contents
        const files = fs.readdirSync(dirPath)

        // Map files with full path and filter only regular files (not directories)
        const fileList = files
            .map(file => path.join(dirPath, file))
            .filter(file => fs.statSync(file).isFile())
            .filter(file => !(file.endsWith('now.json')))
        return fileList
    } catch (error) {
        console.error('Error reading getFilesInDirectory directory:', error)
        return []
    }
}

function compareTimeEarlier(time1, time2) {
    const parts1 = time1.split(':')
    const parts2 = time2.split(':')
    if (Number(parts1[0]) < Number(parts2[0])) {
        return true
    }
    if (Number(parts1[0]) == Number(parts2[0]) && Number(parts1[1]) < Number(parts2[1])) {
        return true
    }
    if (Number(parts1[0]) == Number(parts2[0]) && Number(parts1[1]) == Number(parts2[1]) && Number(parts1[2]) < Number(parts2[2])) {
        return true
    }
    return false
}

function getSPX(filename) {

    let data
    try {
        data = fs.readFileSync(filename, 'utf8')
    } catch (err) {
        console.error(err)
    }

    let spxLast
    let spxTime

    if (data) {

        // Parse the JSON data
        const jsonData = JSON.parse(data)

        spxLast = jsonData.underlying.last
        spxTime = getTimeStringColon(new Date(jsonData.underlying.quoteTime))

        let returnObject
        returnObject = {
            time: spxTime,
            spx: Number(spxLast)
        }
        return returnObject
    }

    return undefined

}

function getTimeStringColon(date) {
    const p = new Intl.DateTimeFormat('en', {
        hour:'2-digit',
        minute:'2-digit',
        second:'2-digit',
        hour12: false
    }).formatToParts(date).reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
    }, {});

    return `${p.hour}:${p.minute}:${p.second}`
}


function getRecommendationForOrder(time) {

    let files = getFilesInRecommendationsDirectory()

    let found

    files.forEach((file) => {
        let fileName = file.split('/').pop();

        // Ensure the file name starts with "recommendation"
        if (fileName.startsWith("recommendation")) {
            let fileTime = fileName.split('.')[0];
            fileTime = fileTime.replace("recommendation-", "").replace(/-/g, ":");

            if (withinMinutes(1, time, fileTime)) {
                found = file;
            }
        }
    });

    return found
}


function getFilesInRecommendationsDirectory() {

    try {
        // Read directory contents
        // let dirPath = SPX_HOME + '/' + DATE + '/omeic/tos/recommendations'

        let dirPath = SPX_HOME + "\\" + DATE
        console.log("updated dirPath:<" + dirPath + ">")




        const files = fs.readdirSync(dirPath);

        // Map files with full path and filter only regular files (not directories)
        const fileList = files
            .map(file => path.join(dirPath, file))
            .filter(file => fs.statSync(file).isFile())
            .filter(file => !(file.endsWith('recommendation.json')))
        return fileList
    } catch (error) {
        console.error('Error reading recommendations directory:', error)
        return []
    }
}

function printResults() {

    let ls

    let totalPL = 0
    let entries = 0
    let winners = 0
    let losers = 0
    let bes = 0

    console.log('\nTOS Live MEIC Results for ' + DATE + ': ')
    for (let i = ics.length - 1; i >= 0; i--) {

        let ic = ics[i]

        totalPL += ic.pl
        entries++
        if (ic.status === 'WINNER' || ic.status === 'EARLY') {
            winners++
        } else if (ic.status === 'BE') {
            bes++
        } else if (ic.status === 'LOSER') {
            losers++
        } else {
            console.log("ERROR: unknown IC status")
        }
    }
    ls = '\nTotal P/L: ' + usCurrency.format(totalPL)
    console.log(ls)
    ls = 'Entries: ' + entries.toFixed(0)
    console.log(ls)
    ls = 'Winners: ' + winners.toFixed(0)
    console.log(ls)
    ls = 'Break Evens: ' + bes.toFixed(0)
    console.log(ls)
    ls = 'Losers: ' + losers.toFixed(0)
    console.log(ls)


    if (itms && itms.length > 0) {
        let itmTotal = 0
        itms.forEach(itm => {
            let _o = symbolToOption(itm.symbol)
            itmTotal += itm.amount
        })
        console.log('\nITM losses: ' + usCurrency.format(itmTotal))
        console.log('Actual P/L: ' + usCurrency.format(totalPL + itmTotal))
    }




    for (let i = ics.length - 1; i >= 0; i--) {

        let ic = ics[i]

        ls = '\n*** IC ' + ic.time + (ic.stopRisk ? ' (' + ic.stopRisk + ') ' : ' ') + ic.status
        ls = ls + ' ' + usCurrency.format(ic.pl)
        console.log(ls)

        let spxAtTime = ic.spxLast

        let putFilledPrice
        let putShortStopPrice
        let putSlippage
        let putLimitPrice
        let putShortPrice
        let putLongPrice
        let putLongSTCPrice
        let putActualNetCredit
        let putStoppedTime
        let putEarlyTime

        putLimitPrice = Number(ic.putSpread.limit).toFixed(2)
        putShortPrice = Number(ic.putSpread.shortSTO).toFixed(2)
        putLongPrice = Number(ic.putSpread.longBTO).toFixed(2)
        putActualNetCredit = Number(ic.putSpread.spreadNetCredit).toFixed(2)

        if (ic.putSpread.shortStopped) {
            putShortStopPrice = ' ' + usCurrency.format(ic.putSpread.stopPrice)
            putFilledPrice = ' ' + usCurrency.format(ic.putSpread.shortBTC)
            putSlippage = ' ' + usCurrency.format(ic.putSpread.shortBTC - ic.putSpread.stopPrice)
            putLongSTCPrice = ' ' + usCurrency.format(ic.putSpread.longSTC)
            putStoppedTime = ' ' + ic.putSpread.stoppedTime
            putEarlyTime = ''
        } else {
            putFilledPrice = ''
            putShortStopPrice = ''
            putSlippage  = ''
            putLongSTCPrice = ''
            putStoppedTime = ''
            putEarlyTime = ic.putSpread.isEarly ? (' ' + ic.putSpread.earlyTime) : ''
        }

        ls = ic.putSpread.spread.putCall + ' '
        ls = ls + ' ' + ic.putSpread.spread.short + '/' + ic.putSpread.spread.long
        if (spxAtTime) {
            ls = ls + ' ' + spxAtTime
        } else {
            // do nothing
        }
        // ls = ls + ' ' + Math.abs(Number(spxAtTime - ic.putSpread.spread.short)).toFixed(0)
        // ls = ls + ' $' + putLimitPrice
        // ls = ls + ' $' + putShortPrice
        // ls = ls + ' $' + putLongPrice
        ls = ls + ' ' + usCurrency.format(putActualNetCredit)
        ls = ls + ' ' + ic.putSpread.status
        ls = ls + '' + putStoppedTime
        ls = ls + '' + putEarlyTime
        // ls = ls + '' + putShortStopPrice
        ls = ls + '' + putFilledPrice
        // ls = ls + '' + putSlippage
        ls = ls + '' + putLongSTCPrice
        ls = ls + ' ' + usCurrency.format(ic.putSpread.pl)
        console.log(ls)

        let callFilledPrice
        let callShortStopPrice
        let callSlippage
        let callLimitPrice
        let callShortPrice
        let callLongPrice
        let callLongSTCPrice
        let callActualNetCredit
        let callStoppedTime
        let callEarlyTime

        callLimitPrice = Number(ic.callSpread.limit).toFixed(2)
        callShortPrice = Number(ic.callSpread.shortSTO).toFixed(2)
        callLongPrice = Number(ic.callSpread.longBTO).toFixed(2)
        callActualNetCredit = Number(ic.callSpread.spreadNetCredit).toFixed(2)

        if (ic.callSpread.shortStopped) {
            callShortStopPrice = ' ' + usCurrency.format(ic.callSpread.stopPrice)
            callFilledPrice = ' ' + usCurrency.format(ic.callSpread.shortBTC)
            callSlippage = ' ' + usCurrency.format(ic.callSpread.shortBTC - ic.callSpread.stopPrice)
            callLongSTCPrice = ' ' + usCurrency.format(ic.callSpread.longSTC)
            callStoppedTime = ' ' + ic.callSpread.stoppedTime
            callEarlyTime = ''
        } else {
            callFilledPrice = ''
            callShortStopPrice = ''
            callSlippage  = ''
            callLongSTCPrice = ''
            callStoppedTime = ''
            callEarlyTime = ic.callSpread.isEarly ? (' ' + ic.callSpread.earlyTime) : ''
        }

        ls = ic.callSpread.spread.putCall
        ls = ls + ' ' + ic.callSpread.spread.short + '/' + ic.callSpread.spread.long
        if (spxAtTime) {
            ls = ls + ' ' + spxAtTime
        } else {
            // do nothing
        }
        // ls = ls + ' ' + Math.abs(Number(spxAtTime - sic.callSpread.spread.short)).toFixed(0)
        // ls = ls + ' $' + callLimitPrice
        // ls = ls + ' $' + callShortPrice
        // ls = ls + ' $' + callLongPrice
        ls = ls + ' ' + usCurrency.format(callActualNetCredit)
        ls = ls + ' ' + ic.callSpread.status
        if (ic.callSpread)
        ls = ls + '' + callStoppedTime
        ls = ls + '' + callEarlyTime
        // ls = ls + '' + callShortStopPrice
        ls = ls + '' + callFilledPrice
        // ls = ls + '' + callSlippage
        ls = ls + '' + callLongSTCPrice
        ls = ls + ' ' + usCurrency.format(ic.callSpread.pl)
        console.log(ls)

    }

    if (itms && itms.length > 0) {
        console.log('\nITM Option Results')
        let itmTotal = 0
        itms.forEach(itm => {
            let _o = symbolToOption(itm.symbol)
            console.log('ITM ' + itm.longShort + ' ' + itm.putCall + ' ' + _o.strike + ' (' + itm.spx_close + ') ' + usCurrency.format(itm.amount))
            itmTotal += itm.amount
        })
//         console.log('Actual P/L for the day including ITM losses: ' + usCurrency.format(totalPL + itmTotal))
    }

}

function exportCSV() {

    let ls

    let totalPL = 0
    let entries = 0
    let winners = 0
    let losers = 0
    let bes = 0

    if (SELECTION === 'ALL' || SELECTION === 'SUMMARY') {

        // count the winners and losers
        for (let i = ics.length - 1; i >= 0; i--) {

            let ic = ics[i]

            totalPL += ic.pl
            entries++
            if (ic.status === 'WINNER' || ic.status === 'EARLY') {
                winners++
            } else if (ic.status === 'BE') {
                bes++
            } else if (ic.status === 'LOSER') {
                losers++
            } else {
                console.log("ERROR: unknown IC status")
            }
        }

        // dump the summary info
        // console.log('MEIC Results:,' + DATE)
        console.log('Entity,Date,What,Value')
        // console.log('SUMMARY,' + DATE + ',Total P/L,' + usCurrency.format(totalPL * 100))
        console.log('SUMMARY,' + DATE + ',Total P/L,' + usCurrency.format(totalPL))
        console.log('SUMMARY,' + DATE + ',Entries,' + entries.toFixed(0))
        console.log('SUMMARY,' + DATE + ',Winners,' + winners.toFixed(0))
        console.log('SUMMARY,' + DATE + ',Break Evens,' + bes.toFixed(0))
        console.log('SUMMARY,' + DATE + ',Losers,' + losers.toFixed(0))
    }

    if (SELECTION === 'ALL' || SELECTION === 'ICS') {

        // export the IC info
        ls = 'Entity,Date,Time,EM,Limit,NetCredit,StopRisk,Status,Gross P/L,Fees,Net P/L'
        console.log(ls)
        for (let i = ics.length - 1; i >= 0; i--) {

            let ic = ics[i]

            let icNetPL = 0
            let icGrossPL = 0
            let icFees = 0
            let icStatus = ic.status
            icNetPL = Number(ic.putSpread.pl) + Number(ic.callSpread.pl)
            icFees = Number(ic.putSpread.shortSTOFees)
                + Number(ic.putSpread.longBTOFees)
                + Number(ic.putSpread.shortBTCFees)
                + Number(ic.putSpread.longSTCFees)
                + Number(ic.callSpread.shortSTOFees)
                + Number(ic.callSpread.longBTOFees)
                + Number(ic.callSpread.shortBTCFees)
                + Number(ic.callSpread.longSTCFees)
            icGrossPL = icNetPL + icFees
            ls = 'IC,' + DATE + ',' + ic.time
            if (ic.em) {
                ls = ls + ',' + Number(ic.em).toFixed(1)
            } else {
                ls = ls + ','
            }
            ls = ls + ',' + usCurrency.format(ic.limit)
            ls = ls + ',' + usCurrency.format(ic.netCredit)
            if (ic.stopRisk) {
                ls = ls + ',' + Number(ic.stopRisk).toFixed(1)
            } else {
                ls = ls + ','
            }
            ls = ls + ',' + icStatus
            // ls = ls + ',' + usCurrency.format(icPL * 100)
            ls = ls + ',' + usCurrency.format(icGrossPL)
            ls = ls + ',' + usCurrency.format(icFees)
            ls = ls + ',' + usCurrency.format(icNetPL)
            console.log(ls)

        }
    }

    if (SELECTION === 'ALL' || SELECTION === 'SPREADS') {

        // export the spread info
        ls = 'Entity,Date,Time,Type,Short,Long,SPX,Offset,Width,Limit,ShortSTO,LongBTO,NetCredit,Outcome,ExitTime,Stop,Filled,Slip,LongSTC,Gross P/L,Fees,Net P/L'
        console.log(ls)
        for (let i = ics.length - 1; i >= 0; i--) {

            let ic = ics[i]

            let spxAtTime = Number(ic.spxLast)

            let spread

            spread = ic.putSpread.spread
            let putExitTime
            let putFilledPrice
            let putShortStopPrice
            let putSlippage
            let putLimit
            let putShortSTO
            let putLongBTO
            let putLongSTC
            let putActualNetCredit
            let putSpreadNetPL
            let putSpreadFees
            let putSpreadGrossPL
            putLimit = ic.putSpread.limit ? usCurrency.format(ic.putSpread.limit) : ''
            putShortSTO = usCurrency.format(ic.putSpread.shortSTO)
            putLongBTO = usCurrency.format(ic.putSpread.longBTO)
            putActualNetCredit = usCurrency.format(ic.putSpread.shortSTO - ic.putSpread.longBTO)
            putSpreadNetPL = Number(ic.putSpread.pl)
            putSpreadFees = Number(ic.putSpread.shortSTOFees)
                + Number(ic.putSpread.longBTOFees)
                + Number(ic.putSpread.shortBTCFees)
                + Number(ic.putSpread.longSTCFees)
            putSpreadGrossPL = putSpreadNetPL + putSpreadFees
            if (ic.putSpread.status === 'STOPPED' && ic.putSpread.stoppedTime) {
                putExitTime = ',' + ic.putSpread.stoppedTime
                putFilledPrice = ',' + usCurrency.format(ic.putSpread.shortBTC)
                putShortStopPrice = ',' + usCurrency.format(ic.putSpread.stopPrice)
                putSlippage = ',' + usCurrency.format(ic.putSpread.shortBTC - ic.putSpread.stopPrice)
                putLongSTC = ',' + usCurrency.format(ic.putSpread.longSTC)
            } else {
                if (ic.putSpread.isEarly) {
                    putExitTime = ',' + ic.putSpread.earlyTime
                } else {
                    putExitTime = ','
                }
                putFilledPrice = ','
                putShortStopPrice = ','
                putSlippage = ','
                putLongSTC = ','
            }
            ls = 'SPREAD,' + DATE + ',' + ic.time + ',' + spread.putCall
            ls = ls + ',' + spread.short + ',' + spread.long
            if (spxAtTime) {
                ls = ls + ',' + spxAtTime.toFixed(0)
                ls = ls + ',' + Math.abs(spxAtTime - spread.short).toFixed(0)
            } else {
                ls = ls + ','
                ls = ls + ','
            }
            ls = ls + ',' + Math.abs(Number(spread.short) - Number(spread.long))
            ls = ls + ',' + putLimit + ',' + putShortSTO + ',' + putLongBTO + ',' + putActualNetCredit
            ls = ls + ',' + ic.putSpread.status + putExitTime
            ls = ls + putShortStopPrice + putFilledPrice + putSlippage + putLongSTC
            // ls = ls + ',' + usCurrency.format(ic.putSpread.pl * 100)
            ls = ls + ',' + usCurrency.format(putSpreadGrossPL)
            ls = ls + ',' + usCurrency.format(putSpreadFees)
            ls = ls + ',' + usCurrency.format(putSpreadNetPL)
            console.log(ls)

            spread = ic.callSpread.spread
            let callExitTime
            let callFilledPrice
            let callShortStopPrice
            let callSlippage
            let callLimit
            let callShortSTO
            let callLongBTO
            let callLongSTC
            let callActualNetCredit
            let callSpreadNetPL
            let callSpreadFees
            let callSpreadGrossPL
            callLimit = ic.callSpread.limit ? usCurrency.format(ic.callSpread.limit) : ''
            callShortSTO = usCurrency.format(ic.callSpread.shortSTO)
            callLongBTO = usCurrency.format(ic.callSpread.longBTO)
            callActualNetCredit = usCurrency.format(ic.callSpread.shortSTO - ic.callSpread.longBTO)
            callSpreadNetPL = Number(ic.callSpread.pl)
            callSpreadFees = Number(ic.callSpread.shortSTOFees)
                + Number(ic.callSpread.longBTOFees)
                + Number(ic.callSpread.shortBTCFees)
                + Number(ic.callSpread.longSTCFees)
            callSpreadGrossPL = callSpreadNetPL + callSpreadFees
            if (ic.callSpread.status === 'STOPPED' && ic.callSpread.stoppedTime) {
                callExitTime = ',' + ic.callSpread.stoppedTime
                callFilledPrice = ',' + usCurrency.format(ic.callSpread.shortBTC)
                callShortStopPrice = ',' + usCurrency.format(ic.callSpread.stopPrice)
                callSlippage = ',' + usCurrency.format(ic.callSpread.shortBTC - ic.callSpread.stopPrice)
                callLongSTC = ',' + usCurrency.format(ic.callSpread.longSTC)
            } else {
                if (ic.callSpread.isEarly) {
                    callExitTime = ',' + ic.callSpread.earlyTime
                } else {
                    callExitTime = ','
                }
                callFilledPrice = ','
                callShortStopPrice = ','
                callSlippage = ','
                callLongSTC = ','
            }
            ls = 'SPREAD,' + DATE + ',' + ic.time + ',' + spread.putCall
            ls = ls + ',' + spread.short + ',' + spread.long
            if (spxAtTime) {
                ls = ls + ',' + spxAtTime.toFixed(0)
                ls = ls + ',' + Math.abs(spxAtTime - spread.short).toFixed(0)
            } else {
                ls = ls + ','
                ls = ls + ','
            }
            ls = ls + ',' + Math.abs(Number(spread.short) - Number(spread.long))
            ls = ls + ',' + callLimit + ',' + callShortSTO + ',' + callLongBTO + ',' + callActualNetCredit
            ls = ls + ',' + ic.callSpread.status + callExitTime
            ls = ls + callShortStopPrice + callFilledPrice + callSlippage + callLongSTC
            // ls = ls + ',' + usCurrency.format(ic.callSpread.pl * 100)
            ls = ls + ',' + usCurrency.format(callSpreadGrossPL)
            ls = ls + ',' + usCurrency.format(callSpreadFees)
            ls = ls + ',' + usCurrency.format(callSpreadNetPL)
            console.log(ls)

        }
    }
}



