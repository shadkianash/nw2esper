import argparse
import sys
import re
import urllib2
import json
import time

parser = argparse.ArgumentParser(prog='nw2esper',
                                 description='Retrieve the metadata of the sessions afetedd by the query and format them to use in the Esper Tryout Page (http://esper-epl-tryout.appspot.com/epltryout/mainform.html)')
parser.add_argument('-s', '--service', dest='service',
                    help='the service that you want to use (e.g. -s http://broker:50103)', required=True)
parser.add_argument('-q', '--query', dest='query',
                    help='query used to retrieve the sessions of your interest (copied from NW investigation debug)',
                    required=True)
parser.add_argument('-u', '--user', dest='username', help='the username to use in the REST API', default='admin')
parser.add_argument('-p', '--password', dest='password', help='the password of the username to use in the REST API',
                    default='netwitness')
parser.add_argument('-o', '--output', dest='output',
                    help='the output file. if is not specified, the output will be the stderr')

args = parser.parse_args()

rex = re.match("(?P<proto>https?)://(?P<service>[^:]+):(?P<port>\d+)", args.service.lower())
if (rex != None):
    PROTOCOL = rex.group('proto')
    SERVER = rex.group('service')
    PORT = rex.group('port')
else:
    sys.stderr.write('error with service url: ' + args.service.lower() + '\n')
    sys.stderr.write('service url must be: http(s)://ip:port\n')
    sys.exit(1)

sys.stderr.write('protocol:' + PROTOCOL + '\nservice: ' + SERVER + '\nport: ' + PORT + '\n')
url_password = urllib2.HTTPPasswordMgrWithDefaultRealm()
sys.stderr.write('username: ' + args.username + '\n')
sys.stderr.write('password:' + args.password + '\n')
url_password.add_password(None, PROTOCOL + "://" + SERVER + ':' + PORT, args.username, args.password)
handler = urllib2.HTTPBasicAuthHandler(url_password)
opener = urllib2.build_opener(handler)
if 'select' in args.query.lower():
    rex = re.match("(?P<select>select) (?P<metas>.* ?)where", args.query, re.IGNORECASE)
    if (rex != None):
        if 'time' in rex.group('metas').lower():
            myquery = args.query
        else:
            sys.stderr.write('time meta is needed to generate time increment in esper')
            exit(1)
    else:
        sys.stderr.write('invalid query sintax')
        exit(1)
else:
    myquery = 'select * where ' + args.query

myquery = urllib2.quote(myquery)
sys.stderr.write('encoded query: ' + myquery + '\n')
urlquery = PROTOCOL + "://" + SERVER + ':' + PORT + '/sdk?msg=query&query=' + myquery + '&force-content-type=application/json'
sys.stderr.write('try url:' + urlquery + '\n')
LastTime = 0
mySchemaList = {}
mySchema = 'CREATE SCHEMA Event('
myStartTime = 't = "1979-02-13 11:45:00.000"'
myCurrentGroup = 0
eventsCount = -1
try:
    site = opener.open(urlquery)
    site = unicode(site.read(), errors='replace')
    events = json.loads(site)
    for event in events:  # cada evento dentro de los eventos
        if 'bla bla' not in event['string'].lower():
            for MetaData in event['results']['fields']:
                if myCurrentGroup != MetaData['group']:
                    myCurrentGroup = MetaData['group']
                    if eventsCount == -1:
                        eventsCount = 0
                    else:
                        myEvent = myEvent[:len(myEvent) - 2]
                        myEvent = myEvent + '}'
                        sys.stderr.write(myEvent + '\n')
                    eventsCount = eventsCount + 1
                    myEvent = 'Event={'
                if MetaData['type'] == "time":
                    CurrentTime = MetaData['value']
                    if LastTime == 0:
                        LastTime = CurrentTime
                    if CurrentTime != LastTime:
                        TimeDelta = CurrentTime - LastTime
                        LastTime = CurrentTime
                        myEvent = 't=t.plus(' + str(TimeDelta) + ' seconds) \n' + myEvent
                        # MetaData['value'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(MetaData['value']))

                MetaData['type'] = MetaData['type'].replace('.', '_')
                mySchemaList[MetaData['type']] = MetaData['format']
                if MetaData['format'] == 65:
                    separator = '\''
                elif MetaData['format'] == 128:
                    separator = '\''
                else:
                    separator = ''
                myEvent = myEvent + MetaData['type'] + '=' + separator + str(MetaData['value']) + separator + ', '

    myEvent = myEvent[:len(myEvent) - 2]
    myEvent = myEvent + '}'
    sys.stderr.write(myEvent + '\n')
    for tempschema, tempvalue in mySchemaList.iteritems():

        mySchema = mySchema + tempschema
        if tempvalue == 2:
            typevar = ' short'
        elif tempvalue == 5:
            typevar = ' integer'
        elif tempvalue == 6:
            typevar = ' long'
        elif tempvalue == 8:
            typevar = ' long'
        elif tempvalue == 32:
            typevar = ' long'
        elif tempvalue == 65:
            typevar = ' string'
        elif tempvalue == 128:
            typevar = ' string'

        mySchema = mySchema + ' ' + typevar + ', '

    mySchema = mySchema[:len(mySchema) - 2] + ');'

    sys.stderr.write('\n' + mySchema + '\n')

    sys.stderr.write('Events Count: ' + str(eventsCount) + ' \n')

except urllib2.URLError, e:
    sys.stderr.write('error while trying query!\n')
    contenidos = e.read()
    sys.stderr.write(contenidos)

