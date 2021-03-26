#!/usr/bin/env python
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------
# Name: uxdbx
# Purpose: database monitoring
#
# Author:      Dennis Spera
#
# Created:     12-June-2018
# RCS-ID:      $Id: $
# Copyright:   (c) 2018 by aSaag Software
# Licence:     wxWindows license
#
#
#----------------------------------------------------------------------

import traceback, re, sys, os
import pprint, sqlite3, time
import cx_Oracle
import config as cfg
from tabulate import tabulate

#=======================================================================================================================
classes = []

class BaseClass(*classes):
    def __init__(self):
        self.instance = {}
        self.iTree = {}
        for className in classes:
            className.__init__(self)

    def _waiters_and_holders(self):
        dbh = obj._get('dbh')

        sql = '''
                SELECT waiting_session,
                       holding_session,
                       lock_type,
                       mode_held,
                       mode_requested
                  FROM dba_waiters
              '''
        try:
            try:
                curs = dbh.cursor()
                curs.execute(sql)
                data = []
                for row in curs.execute(sql):
                    data.append(row)

                return data

            finally:
                curs.close()

        except cx_Oracle.DatabaseError as er:
            print(er.args[0].message)
            sys.exit()

    def _session_state_all(self):
        dbh = obj._get('dbh')
        sql = '''
                SELECT a.username, a.sid, DECODE(a.state, 'WAITING', 'Waiting','Working') state,
                       DECODE(a.state,'WAITING','So far ' | | seconds_in_wait,'Last waited ' | | wait_time / 100) | | ' secs for ' | | event "Description",
                       substr(b.sql_text,0,60)
                FROM v$session a, v$sqlarea b
                WHERE a.sql_id = b.sql_id
                AND a.username NOT IN ('SYS','SYSTEM','DBSNMP','MDSYS','XDB','LBACSYS','GSMADMIN_INTERNAL','CTXSYS','ORDSYS','WMSYS','DVSYS')
                AND a.username IS NOT NULL               
              '''

        try:
            try:
                curs = dbh.cursor()
                curs.execute(sql)
                data = []
                for row in curs.execute(sql):
                    data.append(row)

                return data

            finally:
                curs.close()


        except cx_Oracle.DatabaseError as er:
            print(er.args[0].message)
            sys.exit()

    def _database_locks(self):
        dbh = obj._get('dbh')
        sql = '''
              SELECT lo.session_id,
                     lo.oracle_username,
                     do.object_name,
                     DECODE (lo.locked_mode,
                             0, 'None',
                             1, 'Null',
                             2, 'Row Share (SS)',
                             3, 'Row Excl (SX)',
                             4, 'Share',
                             5, 'Share Row Excl (SSX)',
                             6, 'Exclusive',
                             TO_CHAR (lo.locked_mode))
                        mode_held
                FROM v$locked_object lo, dba_objects do
               WHERE lo.object_id = do.object_id
            ORDER BY 1, 3
              '''

        try:
            try:
                curs = dbh.cursor()
                curs.execute(sql)
                data = []
                for row in curs.execute(sql):
                    data.append(row)

                return data

            finally:
                curs.close()

        except cx_Oracle.DatabaseError as er:
            print(er.args[0].message)
            sys.exit()



    def _add(self, **kwargs):
        if kwargs['_name']  in self.iTree:
            print('attempt to add an existing instance variable : ' + _name)
            exit(1)
        else:
            try:
                if  kwargs['_type']:
                    pass
            except:
                kwargs['_type'] = None

            try:
                if kwargs['_readonly']:
                    pass
            except:
                kwargs['_readonly'] = None

            try:
                if kwargs['_default']:
                    pass
            except:
                kwargs['_default'] = None

            try:
                if  kwargs['_file_name']:
                    pass
            except:
                kwargs['_file_name'] = None

            try:
                if kwargs['_current_line_number']:
                    pass
            except:
                kwargs['_current_line_number'] = None

            try:
                if kwargs['_current_function_name']:
                    pass
            except:
                kwargs['_current_function_name'] = None

            self.instance = {'name':kwargs['_name'], 'type':kwargs['_type'], 'readonly':kwargs['_readonly'], 'default':kwargs['_default'],
                             'file_name':kwargs['_file_name'],'current_line_number':kwargs['_current_line_number'], 'current_function_name':kwargs['_current_function_name']}

            self.iTree[ kwargs['_name'] ] = self.instance

    def _set(self, _name, value):
        if _name in self.iTree:
            self.iTree[_name][_name] = value
        else:
            print('instance variable ' + _name + ' does not exist')
            self._exit()

    def _get(self, _name):
        if _name in self.iTree[_name]:
           return self.iTree[_name][_name]
        else:
           print('instance variable ' + _name + ' does not exist')
           self._exit()

    def _destroy(self, _name):
        if _name in self.iTree:
           del self.iTree[_name][_name]
        else:
           print('instance variable ' + _name + ' does not exist')
           self._exit()

    def _undef(self, _name):
        if _name in self.iTree:
           del self.iTree[_name][_name]
        else:
           print('instance variable ' + _name + ' does not exist')
           self._exit()

    def _dump (self):
        print("\n",'Dump of object')
        print('===============================================================')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.iTree)
        print('===============================================================')

    def _exit(self):
        traceback.print_stack()
        print(repr(traceback.extract_stack()))
        print(repr(traceback.format_stack()))
        exit(1)

    def _dbh(self):
        conn = cx_Oracle.connect(cfg.username, cfg.password, cfg.dsn, encoding=cfg.encoding)
        self._add(_name='dbh', _type='integer', _readonly='readonly', _default=0)
        self._set('dbh', conn)

    def _format(self, data, columns,report):

        size=tabulate(data, columns, tablefmt="pipe")
        list_of_tuples = list(data)
        records=len(list_of_tuples)
        title_length=int(   len(size)/(records+2))
        spacing = 4
        hline = '+' + ((title_length -2) * '-') + '+'
        padding = int((title_length -(len(report)+spacing))/2)
        pad = ' ' * padding

        print (hline)
        print ("+ " + pad + report + pad + " +" )
        print (hline)

        print(tabulate(data, columns, tablefmt="pipe"))
        print (hline)
        print ("")

    def _header_only(self, report, size):

        title_length=size
        spacing = 4
        hline = '+' + ((title_length -2) * '-') + '+'
        padding = int((title_length -(len(report)+spacing))/2)
        pad = ' ' * padding

        print (hline)
        print ("+ " + pad + report + pad + " +" )
        print (hline)


def new():
        d = {}
        for class_name in classes:
            list = dir(class_name)
            for method in list:
                regex = r"(^__(.*)__$)"
                if not re.search(regex, method):
                    if method not in d:
                        d[method] = None
                    else:
                        pass
                        #print('method collision ' + method + ' is defined')
                        #exit(1)

        return BaseClass()



#-------------
#   M A I N
#-------------




if __name__ == "__main__":

    app = BaseClass()
    obj = new()
    obj._dbh()

    for i in range(1,5):
     os.system('clear')

     data = obj._waiters_and_holders()
     obj._format (data, ('waiter','holder','type','held','requested'),'waiters and holders')

     data = obj._database_locks()
     obj._format (data, ('session','username','object','mode held'),'locking')

     data = obj._session_state_all()
     obj._format (data, ('username','sid','state','description','sql'), 'sessions waiting')

     obj._header_only('system metrics', 40)
     os.system('sar -u 1 1')

     obj._header_only('system free mempry', 40)
     os.system('free -m')

     obj._header_only('top memory consumers', 40)
     os.system('ps axwwo "pid user size rss etime cputime args" | sort -n -k +4 | tail -10 | sort -n -r -k +4')

     obj._header_only('network lag to database', 40)
     os.system('tnsping ' + cfg.dsn + ' 5' + ' | grep sec')

     time.sleep(5)

     
     if os.path.isfile('stop'):
        print ('stop file found...exiting') 
        sys.exit(0) 