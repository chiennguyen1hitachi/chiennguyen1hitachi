import boto3
import json
import math
from db_util import get_thing_id, get_thing_property_value

client = boto3.client('iot-data', region_name='us-west-2')

def lambda_handler(event, context):
    alias = event.get('alias', None)
    value = event.get('value', None)
    timestamp = event.get('timestamp', None)
    quality = 'GOOD'
    id = alias.split("/")[3]

    thing_id = get_thing_id(['DeviceId', id])
    if thing_id is not None and len(thing_id) > 0:
        thing_id = thing_id[0]
        property_names = ['Enable KPI monitoring', 'Include Warning Threshold For Kpi Health', 
        'Upper Warning Threshold', 'Lower Warning Threshold',
        'Upper Critical Threshold', 'Lower Critical Threshold']
        is_enable_kpi = get_thing_property_value([thing_id, property_names[0]])
        if len(is_enable_kpi) > 0 and is_enable_kpi[0][0] == 'true':
            consider_warning_threshold_for_kpi_health = get_thing_property_value([thing_id, property_names[1]])
            if len(consider_warning_threshold_for_kpi_health) > 0 and consider_warning_threshold_for_kpi_health[0][0] == 'true':
                upper_warning_threshold = get_thing_property_value([thing_id, property_names[2]])
                lower_warning_threshold = get_thing_property_value([thing_id, property_names[3]])
                if (len(upper_warning_threshold) > 0 and value >= float(upper_warning_threshold[0][0])) or (len(lower_warning_threshold) > 0 and value <= float(lower_warning_threshold[0][0])):
                    quality = 'BAD'
            else:
                upper_critical_threshold = get_thing_property_value([thing_id, property_names[4]])
                lower_critical_threshold = get_thing_property_value([thing_id, property_names[5]])
                if (len(upper_critical_threshold) > 0 and value >= float(upper_critical_threshold[0][0])) or (len(lower_critical_threshold) > 0 and value <= float(lower_critical_threshold[0][0])):
                    quality = 'BAD'
                    
    response = client.publish(
        topic='mfi/sensor-data/enriched-value',
        qos=1,
        payload=json.dumps({
            "alias": alias,
            "value": value,
            "quality": quality,
            "timeInseconds": math.floor(timestamp / 1E3),
            "offsetInNanos": timestamp % 1E3 * 1E6
        })
    )
    return response
    

