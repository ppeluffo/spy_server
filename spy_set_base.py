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
import logging
from spy_bd_general import BD

# Creo un logger local child.
LOG = logging.getLogger(__name__)

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
        except:
            LOG.info('[%s] ERROR: pwrs_unpack [%s]' % ( self.dlgid, form.getvalue('PWRS') ))
            
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
        LOG.info('[%s] %s tpoll=%s' % ( self.dlgid, tag, self.tpoll ))
        LOG.info('[%s] %s tdial=%s' % ( self.dlgid, tag, self.tdial ))
        LOG.info('[%s] %s pwrs_modo=%s' % ( self.dlgid, tag, self.pwrs_modo ))
        LOG.info('[%s] %s pwrs_start=%s' % ( self.dlgid, tag, self.pwrs_start ))
        LOG.info('[%s] %s pwrs_end=%s' % ( self.dlgid, tag, self.pwrs_end ))
        LOG.info('[%s] %s dist=%s' % ( self.dlgid, tag, self.dist ))
             
        if longformat is not True:
            return
        LOG.info('[%s] %s imei=%s' % ( self.dlgid, tag, self.imei ))
        LOG.info('[%s] %s simid=%s' % ( self.dlgid, tag, self.simid ))
        LOG.info('[%s] %s ver=%s' % ( self.dlgid, tag, self.ver ))
        LOG.info('[%s] %s uid=%s' % ( self.dlgid, tag, self.uid ))
        LOG.info('[%s] %s csq=%s' % ( self.dlgid, tag, self.csq ))
        LOG.info('[%s] %s wrst=%s' % ( self.dlgid, tag, self.wrst ))
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
        base_response = ''
        if self.tpoll != other.tpoll:
           base_response += 'TPOLL=%s:' % self.tpoll
           
        if self.tdial != other.tdial:
            base_response += 'TDIAL=%s:' % self.tdial
            
        if ( self.pwrs_modo != other.pwrs_modo or
            self.pwrs_start != other.pwrs_start or
            self.pwrs_end != other.pwrs_end ):
            base_response += 'PWRS=%s,%s,%s:' % ( self.pwrs_modo, self.pwrs_start, self.pwrs_end )

        if self.dist != other.dist:
            base_response += 'DIST=%s:' % self.dist
            
        LOG.info('[%s] confbase_RSP: [%s]' % ( self.dlgid, base_response))
        return(base_response) 
 
       
    def update_bd(self):
        '''
        La configuracion base se guarda en la BD para tener los datos actualizados
        de c/datalogger
        Preparo un dict donde paso todos los parametros a actualizar.
        '''
        bd = BD()
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
        
        bd.update(self.dlgid, d)
        return
    
    
