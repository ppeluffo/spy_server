#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 21:43:48 2019

@author: pablo

    - El parametros DIST el datalogger lo manda como ON u OFF.
    - En la base de datos esta 0 (OFF) o 1 (ON)
    - Idem con PWRS
    * OJO con los types: Tanto al leer de la BD como del QS debo convertirlos
      al mismo tipo para que anden las comparaciones
"""

import os
import cgi
import re
from spy_log import log

#------------------------------------------------------------------------------

class Confbase:
    '''
    Crea la estructura y metodos para manejar la configuracion base de todo
    datalogger.
    imei
    version
    uid
    ...
    '''
    def __init__(self, dlgid):
        self.dlgid = dlgid
        self.tpoll = ''
        self.tdial = ''
        self.pwrs_modo = ''
        self.pwrs_start = ''
        self.pwrs_end = ''
        self.imei = ''
        self.simid = ''
        self.ver = ''
        self.uid = ''
        self.cqs = ''
        self.wrst = ''
        self.dist = ''
        return
 
    
    def init_from_qs(self):
        '''
        Recibo un query string que lo parseo y relleno los campos del piloto
        getvalue retorna None si no hay nada
        '''
        form = cgi.FieldStorage()
        self.imei = form.getfirst('IMEI', '0')
        self.ver = form.getfirst('VER', '0')
        self.uid = form.getfirst('UID','0')
        self.simid = form.getfirst('SIMID', '0')
        self.tpoll = int(form.getfirst('TPOLL', '0'))
        self.tdial = int(form.getfirst('TDIAL', '0'))
        try:
            self.pwrs_modo, self.pwrs_start, self.pwrs_end = re.split('=|,', form.getfirst('PWRS','OFF,00,00'))
            self.pwrs_start = int(self.pwrs_start)
            self.pwrs_end = int(self.pwrs_end)
        except Exception as err_var:
            log(module=__name__, function='init_from_qs', level='INFO', dlgid=self.dlgid, msg='ERROR: pwrs_unpack {}'.format(form.getvalue('PWRS')))
            log(module=__name__, function='init_from_qs', level='INFO', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))

        self.csq = form.getfirst('CSQ', '0')
        self.wrst = form.getfirst('WRST', '0')
        self.dist = form.getfirst('DIST', 'OFF')
        return


    def log(self, longformat=False, tag='' ):
        '''
        Los datos de un frame init ya sean del query string o de la BD quedan
        en un dict.
        Aqui los logueo en 2 formatos:
            corto: solo los parametros que se van a usar para reconfigurar
            largo: todos los parametros ( incluso los que son solo informativos )
        '''
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} tpoll={1}'.format(tag, self.tpoll))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} tdial={1}'.format(tag, self.tdial))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} pwrs_modo={1}'.format(tag, self.pwrs_modo))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} pwrs_start={1}'.format(tag, self.pwrs_start))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} pwrs_end={1}'.format(tag, self.pwrs_end))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} dist={1}'.format(tag, self.dist))

        if longformat is not True:
            return
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} imei={1}'.format(tag, self.imei))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} simid={1}'.format(tag, self.simid))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} ver={1}'.format(tag, self.ver))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} uid={1}'.format(tag, self.uid))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} csq={1}'.format(tag, self.csq))
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} wrst={1}'.format(tag, self.wrst))
        return
 
              
    def init_from_bd( self, dcnf ):
        '''
        Leo la configuracion base del dlgid desde la base de datos y relleno la estructura self.
        La base de datos ya esta leida y los valores pasados en el dcnf.
        '''
        # Convierto el dict a la estructura Cbase
        self.tpoll = int(dcnf.get(('BASE','TPOLL'), 0))
        self.tdial = int(dcnf.get(('BASE','TDIAL'),0))
        self.pwrs_modo = int(dcnf.get(('BASE','PWRS_MODO'),0))
        if self.pwrs_modo == 0:
            self.pwrs_modo = 'OFF'
        else:
            self.pwrs_modo = 'ON'
        self.pwrs_start = int(dcnf.get(('BASE','PWRS_HHMM1'),0))
        self.pwrs_end = int(dcnf.get(('BASE','PWRS_HHMM2'),0))
        
        self.imei = dcnf.get(('BASE','IMEI'),0)
        self.simid = dcnf.get(('BASE','SIMID'),0)
        self.ver = dcnf.get(('BASE','FIRMWARE'),0)
        self.uid = dcnf.get(('BASE','UID'),0)
        self.csq = ''
        self.wrst = ''
        self.dist =  int(dcnf.get(('BASE','DIST'),0))
        if self.dist == 0:
            self.dist = 'OFF'
        else:
            self.dist = 'ON'
        return


    def __str__(self):
        response = 'CBASE: dlgid=%s, ' % ( self.dlgid )
        response += 'imei=%s, ' % ( self.imei )
        response += 'version=%s, ' % ( self.ver )
        response += 'uid=%s, ' % ( self.uid )
        response += 'simid=%s, ' % ( self.simid )
        response += '{ tpoll=%s, ' % ( self.tpoll )
        response += 'tdial=%s, ' % ( self.tdial )
        response += 'pwrs_modo=%s, ' % ( self.pwrs_modo )
        response += 'pwrs_start=%s, ' % ( self.pwrs_start )
        response += 'pwrs_end=%s }, ' % ( self.pwrs_end )
        response += 'csq=%s, ' % ( self.csq )
        response += 'wrst=%s, ' % ( self.wrst )
        return(response)
 
       
    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.tpoll == other.tpoll and
            self.tdial == other.tdial and
            self.pwrs_modo == other.pwrs_modo and
            self.pwrs_start == other.pwrs_start and
            self.pwrs_end == other.pwrs_end and
            self.dist == other.dist ):
            return True
        else:
            return False
 
          
    def get_response_string(self, other ):
        '''
        Genero el string que deberia mandarse al datalogger con la configuracion
        en la BD del piloto para ese dlgid
        El objeto sobre el cual se llama debe ser la referencia de la BD.
        El 'other' es el objeto con los datos del datalogger
        '''
        response = ''
        if self.tpoll != other.tpoll:
           response += 'TPOLL=%s:' % self.tpoll
           
        if self.tdial != other.tdial:
            response += 'TDIAL=%s:' % self.tdial
            
        if ( self.pwrs_modo != other.pwrs_modo or
            self.pwrs_start != other.pwrs_start or
            self.pwrs_end != other.pwrs_end ):
            response += 'PWRS=%s,%s,%s:' % ( self.pwrs_modo, self.pwrs_start, self.pwrs_end )

        if self.dist != other.dist:
            response += 'DIST=%s:' % self.dist
            
        log(module=__name__, function='get_response_string', level='SELECT', dlgid=self.dlgid,msg='confbase_RSP: {}'.format(response))
        return(response)
 
       
    def get_data_for_update(self):
        '''
        La configuracion base se guarda en la BD para tener los datos actualizados
        de c/datalogger
        Preparo un dict donde paso todos los parametros a actualizar.
        '''
        d = dict()
        try:
            d['IPADDRESS'] = cgi.escape(os.environ["REMOTE_ADDR"])
        except:
            d['IPADDRESS'] = '0.0.0.0'
            
        d['RCVDLINE'] = os.environ['QUERY_STRING']
        d['FIRMWARE'] = self.ver
        d['IMEI'] = self.imei
        d['CSQ'] = self.csq
        d['UID'] = self.uid
        d['SIMID'] = self.simid
        d['COMMITED_CONF'] = 0

        return d
    
    
