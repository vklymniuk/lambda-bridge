const _ = require('lodash');

class LambdaBridge /*implements IPubSub*/ {
    constructor(snsClient, awsAccountId, awsRegion, environment) {
        this._SNS = snsClient;
        this._region = awsRegion;
        this._accountId = awsAccountId;
        this._environment = environment || "";
        this._eventToCallbackMap = new Map();
    }

    subscribe(eventName, cb) {
        this._eventToCallbackMap.set(eventName, cb);
    }

    publish(eventName, event) {
        return this._SNS.publish({
            Message: JSON.stringify(event),
            TopicArn: this._getTopicArn(eventName),
        }).promise();
    }

    _getTopicArn(eventName) {
        return `arn:aws:sns:${this._region}:${this._accountId}:E_${eventName}_${_.toUpper(this._environment)}`;
    }

    _validateEventParameters(eventName, event){
        if(typeof(eventName) != 'string'){
            throw `LambdaBridge.publish: Got invalid event name type. given type: ${typeof(eventName)}`;
        }

        if(typeof(event) != 'object'){
            throw `LambdaBridge.publish: Got invalid event type. given type: ${typeof(event)}`;
        }
    }

    async onSnsEvent(lambdaEvent) {
        try {
            let event = this._parseSNSEvent(lambdaEvent);
            await this.onEvent(event);
            return {
                statusCode: 200,
            }
        }
        catch(e) {
            return {
                body: JSON.stringify(e),
                statusCode: 500,
            };
        }
    }

    _parseSNSEvent(lambdaEvent){
        return JSON.parse(lambdaEvent.Records[0].Sns.Message);
    }

    onContext(ctx) {
        const cb = this._getEventCallback(ctx.EVENT.NAME, ctx.EVENT);

        return cb(ctx);
    }

    onEvent(event){
        const cb = this._getEventCallback(event.NAME, event);

        return cb(event);
    }

    _getEventCallback(eventName, event) {
        this._validateEventParameters(eventName, event);
        let cb = this._eventToCallbackMap.get(eventName);

        if (typeof(cb) != 'function') {
            throw `LambdaBridge.publish: Invalid callback for ${eventName}. callback type ${typeof(cb)}`;
        }

        return cb;
    }
};

module.exports = LambdaBridge;