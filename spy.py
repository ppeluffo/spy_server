#!/usr/bin/python3 -u
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:04:50 2019

@author: pablo

Archivo ppal del SPY_SERVER
Es invocado por los dataloggers al trasitir un frame CGI.
Determina el QUERY STRING y en base al tipo de frame, invoca al modulo
correspondiente

Version 1.0 @ 2019-07-19
 
Ref:
    Consulta sql sin resultados
    https://stackoverflow.com/questions/19448559/sqlalchemy-query-result-is-none
    
    Evaluacion de resultados como instancias
    https://www.programiz.com/python-programming/methods/built-in/isinstance
    
    Errores en Apache
    https://blog.tigertech.net/posts/apache-cve-2016-8743/
    https://serverfault.com/questions/853103/500-internal-server-ah02429-response-header-name
    
    Redis:
    https://realpython.com/python-redis/#using-redis-py-redis-in-python
    https://redis-py.readthedocs.io/en/latest/genindex.html

Advertencias:
    1) init_from_qs: Al leer un cgi si clave ( form.getvalue ) retorna None
    Conviene leer con gerfirst por si retornara una lista, no genera error: more safe.
    Devuelve None como default.
    2) init_from_qs: Al hacer un split, usar un try por si el unpack devuelve diferente cantidad
       de variables de las que se esperan
    3) init_from_bd: Los diccionarios al acceder a claves que no existen retornan error por
       lo que usamos el metodo get o try.
    4) La redis devuelve bytes por lo que debemos convertirlos a int para compararlos
       Los strings que maneja la redis los devuleve como bytes por lo tanto para usarlos como
       str debo convertirlos con decode().
"""

import os
#import cgitb
import configparser
import sys
import spy_log as log

#----------------------------------------------------------------------------- 
#cgitb.enable()
#
Config = configparser.ConfigParser()
Config.read('spy.conf')
#    
#------------------------------------------------------------------------------

if __name__ == '__main__':
   
    # Lo primero es configurar el logger   
    LOG = log.config_logger()
 
    query_string = ''
    # Atajo para debugear x consola ( no cgi )!!!
    if len(sys.argv) > 1:
        if sys.argv[1] == 'DEBUG_SCAN':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_scan']
            os.environ['QUERY_STRING'] = query_string
            print('TEST SCAN: query_string={}'.format( query_string) )

        elif sys.argv[1] == 'DEBUG_IAUX0':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_iaux0']
            os.environ['QUERY_STRING'] = query_string
            print('TEST IAUX0: query_string={}'.format( query_string) )            

        elif sys.argv[1] == 'DEBUG_DATA':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_data']
            os.environ['QUERY_STRING'] = query_string
            print('TEST DATA: query_string={}'.format( query_string) ) 
            
        elif sys.argv[1] == 'DEBUG_INIT':
            # Uso un query string fijo de test del archivo .conf
            query_string = Config['DEBUG']['debug_init']
            os.environ['QUERY_STRING'] = query_string
            print('TEST INIT: query_string={}'.format( query_string) ) 
            
    else:
        # Leo del cgi
        query_string = os.environ.get('QUERY_STRING')
        
    LOG.info('RX:[%s]' % query_string )
        
    # Procesamiento normal x cgi.
    if 'SCAN' in query_string:
        from spy_frame_scan import SCAN_frame
        scan_frame = SCAN_frame()
        scan_frame.process()
      
    elif 'IAUX0' in query_string:
        from spy_frame_iaux0 import IAUX0_frame
        iaux0 = IAUX0_frame()
        iaux0.process()

    elif 'CTL' in query_string:
        from spy_frame_data import DATA_frame
        data = DATA_frame()
        data.processFrame()

    elif 'INIT' in query_string:
        from spy_frame_init import INIT_frame
        init = INIT_frame()
        init.process_frame()

    else:
        LOG.info('RX FRAME ERROR' )
        print('Content-type: text/html\n')
        print('<html><body><h1>ERROR</h1></body></html>')
    
    

      
        

