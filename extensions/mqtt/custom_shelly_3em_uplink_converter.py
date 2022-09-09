from time import time
import re

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log

class CustomShelly3EmUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    def convert(self, topic, body):
        try:
            matched = re.search(
                r'shellies/(shellyem3-\w*?)/(relay|emeter)/(\d)/?(\w*)?', topic)
            if matched:
                dict_result = {}
                name = matched.group(1)
                dict_result["deviceType"] = "Shelly_EM3"
                dict_result["telemetry"] = []
                if matched.group(2) == 'emeter':
                    key = matched.group(4)
                else:
                    key = 'relay_state'
                dict_result["deviceName"] = name
                em_status = {"ts": int(time() * 1000), "values": {key + '_ch' + matched.group(3): body}}
                dict_result["telemetry"].append(em_status)

                log.info('send %s [%s] telemetry for %s value %s', dict_result["deviceName"], dict_result["deviceType"],
                         key, body)
                log.info(dict_result)
                return dict_result
            else:
                log.info('no matches on topic %s', topic)
                return []
        except Exception as e:
            log.exception('Error in converter message: \n%s\n', body)
            log.exception(e)
