from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

def initialize_analyticsreporting(KEY_FILE_LOCATION):
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
    return analytics

def get_report(analytics,ga_report_config):
    return analytics.reports().batchGet(
      body={
        'reportRequests': [ga_report_config]
      }).execute()

def get_data(response):
    # get report data
    report = response.get('reports')[0]
    rows = report.get('data').get('rows')
    
    data = []
    if rows:
        for i in range(len(rows)):
            dim_copy = rows[i].get('dimensions')[:]
            met_copy = rows[i].get('metrics')[0].get('values')
            dim_copy.extend(met_copy)
            data.append(dim_copy)

    return data

def add_n_days(date_, n):
    a = datetime.strptime(date_, '%Y-%m-%d')+timedelta(days=n)
    return a.strftime('%Y-%m-%d')

def get_unsampled_report(analytics, ga_report_config):
    response = get_report(analytics,ga_report_config)
    #headers
    report = response.get('reports')[0]
    columnHeader = report.get('columnHeader')
    dimensionHeaders = columnHeader.get('dimensions')
    metricHeaders = columnHeader.get('metricHeader').get('metricHeaderEntries')
    headers = dimensionHeaders[:]
    for i in range(len(metricHeaders)):
        headers.append(metricHeaders[i].get('name'))
    #unsampling
    if response.get('reports')[0].get('data').get('samplingSpaceSizes'):
        print('Sampling! Solving..')
        start_date = ga_report_config['dateRanges'][0]['startDate']
        end_date = ga_report_config['dateRanges'][0]['endDate']
        if start_date == end_date:
            print('1 day data is still sampled')
            data = get_data(response)
            rowCount = report.get('data').get('rowCount')
            if rowCount:
                if rowCount > 100000:
                    for i in range(rowCount//100000):
                        print('More data coming...' + str(100000*(i+1))+'+')
                        ga_report_config_ = ga_report_config.copy()
                        ga_report_config_["pageToken"] = str(100000*(i+1))
                        response = get_report(analytics, ga_report_config_)
                        data_ = get_data(response)
                        data.extend(data_)
            return data, headers
        ga_report_antisample = {
          "dateRanges": [
            {
              "startDate": start_date,
              "endDate": end_date
            }
          ],
          "metrics": [
            {
              "expression": "ga:sessions"
            }
          ],
          "dimensions": [
            {
              "name": "ga:date"
            }
          ],
          "viewId": ga_report_config['viewId'],
          "samplingLevel": "LARGE",
          "pageSize": 100000
        }
        resp_for_analysis = get_report(analytics,ga_report_antisample)
        rows_for_analysis = resp_for_analysis.get('reports')[0].get('data').get('rows')
        sessions_by_days = []
        for i in range(len(rows_for_analysis)):
            sessions_by_days.append(int(rows_for_analysis[i].get('metrics')[0].get('values')[0]))
        number_of_days = 500000//max(sessions_by_days)
        if number_of_days == 0:
            print('Couldnt get unsampled report')
        else:
            start_date_new = start_date
            end_date_new = add_n_days(start_date_new,number_of_days-1)
            data = []
            #print()
            while (start_date_new <= end_date and end_date_new <= end_date):
                print('from '+start_date_new + ' to '+ end_date_new + ' - loading...')
                ga_report_config_ = ga_report_config.copy()
                ga_report_config_['dateRanges'][0]['startDate'] = start_date_new
                ga_report_config_['dateRanges'][0]['endDate'] = end_date_new
                response_ = get_report(analytics, ga_report_config_)
                if response_.get('reports')[0].get('data').get('samplingSpaceSizes'):
                    if number_of_days > 1:
                        print('still sampling.. partition by day')
                        for i in range(number_of_days):
                            print(str(i)+' day..')
                            ga_report_config_ = ga_report_config.copy()
                            ga_report_config_['dateRanges'][0]['startDate'] = add_n_days(start_date_new,i)
                            ga_report_config_['dateRanges'][0]['endDate'] = add_n_days(start_date_new,i)
                            response__ = get_report(analytics, ga_report_config_)
                            data.extend(get_data(response__))
                            if response__.get('reports')[0].get('data').get('samplingSpaceSizes'):
                                print('Bad things happen - code is shit and data is sampled!')
                    else:
                        print('Bad things happen - code is shit and data is sampled!')
                        data.extend(get_data(response_))
                else:
                    data.extend(get_data(response_))
                rowCount = response_.get('reports')[0].get('data').get('rowCount')
                if rowCount:
                    if rowCount > 100000:
                        for i in range(rowCount//100000):
                            print('More data coming...' + str(100000*(i+1))+'+')
                            ga_report_config_ = ga_report_config.copy()
                            ga_report_config_['dateRanges'][0]['startDate'] = start_date_new
                            ga_report_config_['dateRanges'][0]['endDate'] = end_date_new
                            ga_report_config_["pageToken"] = str(100000*(i+1))
                            response_ = get_report(analytics, ga_report_config_)
                            data.extend(get_data(response_))
                start_date_new = add_n_days(end_date_new,1)
                end_date_new = add_n_days(start_date_new,number_of_days-1)
                if end_date_new > end_date:
                    end_date_new = end_date
    else:
        data = get_data(response)
        rowCount = report.get('data').get('rowCount')
        if rowCount:
            if rowCount > 100000:
                for i in range(rowCount//100000):
                    print('More data coming...' + str(100000*(i+1))+'+')
                    ga_report_config_ = ga_report_config.copy()
                    ga_report_config_["pageToken"] = str(100000*(i+1))
                    response = get_report(analytics, ga_report_config_)
                    data_ = get_data(response)
                    data.extend(data_)

    return data, headers