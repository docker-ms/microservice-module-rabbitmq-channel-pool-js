'use strict';

const os = require('os');

module.exports = (consulKVRabbitMQClusterKey, requiredExchanges, serviceTagSuffix) => {

  if (!consulKVRabbitMQClusterKey || typeof consulKVRabbitMQClusterKey !== 'string') {
    throw new Error('Invalid consul cluster KV store key for RabbitMQ cluster info.');
  }

  if (!serviceTagSuffix || typeof serviceTagSuffix !== 'string') {
    throw new Error('Invalid service tag suffix.');
  }

  serviceTagSuffix = serviceTagSuffix.startsWith('-') ? serviceTagSuffix : '-' + serviceTagSuffix;

  const amqplib = require('amqplib');

  const Promise = require('bluebird');

  const utils = require('microservice-utils');
  const consul = require('microservice-consul');

  return utils.pickRandomly(consul.agents).kv.get(consulKVRabbitMQClusterKey).then((res) => {

    if (!res) {
      return Promise.reject('No RabbitMQ cluster record found in consul kv store by this key.');
    }

    const rabbitmqClusterInfo = JSON.parse(res.Value);

    const _reconstructExchangeAndMqNames = () => {
      for (let ex in rabbitmqClusterInfo.settings.exchanges) {
        rabbitmqClusterInfo.settings.exchanges[ex].name += serviceTagSuffix;
        for (let bind in rabbitmqClusterInfo.settings.exchanges[ex].binds) {
          rabbitmqClusterInfo.settings.exchanges[ex].binds[bind].mq.name += (serviceTagSuffix + '@' + os.hostname());
        }
      }
    };

    return Promise.join(
      utils.queryServicesByServiceNamePrefix(
        utils.pickRandomly(consul.agents),
        [rabbitmqClusterInfo.rabbitmqConsulServicesNamePrefix]
      ),
      _reconstructExchangeAndMqNames(),
      (rabbitmqNodes) => {
        return Promise.resolve(rabbitmqNodes);
      }
    ).then((rabbitmqNodes) => {

      const doConnect = rabbitmqNodes[rabbitmqClusterInfo.rabbitmqConsulServicesNamePrefix].reduce((acc, curr) => {
        acc.push(
          Promise.resolve(
            amqplib.connect(rabbitmqClusterInfo.plainAuth + curr)
          ).reflect()
        );
        return acc;
      }, []);

      const doConfirmChannelCreation = [];

      return Promise.all(doConnect).each((inspection) => {
        if (inspection.isFulfilled()) {
          doConfirmChannelCreation.push(
            Promise.resolve(
              inspection.value().createConfirmChannel()
            ).reflect()
          );
        }
      }).return(doConfirmChannelCreation);

    }).then((doConfirmChannelCreation) => {
      const channels = [];
      return Promise.all(doConfirmChannelCreation).each((inspection) => {
        if (inspection.isFulfilled()) {
          channels.push(inspection.value());
        }
      }).return(channels);
    }).then((channels) => {

      if (!Array.isArray(requiredExchanges) || requiredExchanges.length === 0) {
        return Promise.resolve({
          channels: channels,
          settings: rabbitmqClusterInfo.settings
        });
      }

      const _assertExAndQThenBind = (channel) => {
        const doAssertExAndQ = [];
        requiredExchanges.forEach((ex) => {
          doAssertExAndQ.push(
            channel.assertExchange(
              rabbitmqClusterInfo.settings.exchanges[ex].name,
              rabbitmqClusterInfo.settings.exchanges[ex].type,
              rabbitmqClusterInfo.settings.exchanges[ex].opts
            )
          );
          for (let bind in rabbitmqClusterInfo.settings.exchanges[ex].binds) {
            doAssertExAndQ.push(
              channel.assertQueue(
                rabbitmqClusterInfo.settings.exchanges[ex].binds[bind].mq.name,
                rabbitmqClusterInfo.settings.exchanges[ex].binds[bind].mq.opts
              )
            );
          }
        });
        return Promise.props(doAssertExAndQ).then(() => {
          // The resolved value doesn't matter, only if there is no error happening, then it is perfect.
          const doQBind = [];
          requiredExchanges.forEach((ex) => {
            for (let bind in rabbitmqClusterInfo.settings.exchanges[ex].binds) {
              doQBind.push(
                channel.bindQueue(
                  rabbitmqClusterInfo.settings.exchanges[ex].binds[bind].mq.name,
                  rabbitmqClusterInfo.settings.exchanges[ex].name,
                  rabbitmqClusterInfo.settings.exchanges[ex].binds[bind].routingKey
                )
              );
            }
          });
          return Promise.all(doQBind);
        });
      };

      const doAssertExAndQThenBind = channels.reduce((acc, curr) => {
        acc.push(_assertExAndQThenBind(curr).reflect());
        return acc;
      }, []);

      const flawedChannelIdx = [];

      return Promise.all(doAssertExAndQThenBind).each((inspection, idx) => {
        if (!inspection.isFulfilled()) {
          flawedChannelIdx.push(idx);
        }
      }).then(() => {
        if (flawedChannelIdx.length) {
          for (let i = flawedChannelIdx.length - 1; i >= 0; i--) {
            channels.splice(flawedChannelIdx[i], 1);
          }
        }
        return Promise.resolve({
          channels: channels,
          settings: rabbitmqClusterInfo.settings
        });
      });

    });

  });

};


