#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 08:05:36 2019

@author: pablo
"""

import cgi
import logging
import re
from spy_log import log

#------------------------------------------------------------------------------
class DigitalChannel():
    
    
    def __init__(self, ach_id, dlgid):
        self.dlgid = dlgid
        self.id = ach_id
        self.name = 'X'
        self.tpoll = 0
 
       
    def init_from_qs ( self, ch_id ):
        '''
        qs es el string parseado tipo D0=DIN,250
        Primero vemos que no sea vacio.
        Luego lo parseamos y rellenamos los campos del self.
        '''
        form = cgi.FieldStorage()
        field = form.getfirst(ch_id,'X,0')    # Los canales que no estan los asumo apagados 'X'
        if field is not None:
            try:
                self.name, self.tpoll = re.split('=|,', field ) 
                self.tpoll = int(self.tpoll)
            except Exception as err_var:
                log(module=__name__, function='init_from_qs', level='INFO', dlgid=self.dlgid, msg='ERROR: {0}_unpack {1}'.format(ch_id, field))
                log(module=__name__, function='init_from_qs', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
        return

    
    def log ( self, tag = ''):
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} id={1}: name={2} tpoll={3}'.format(tag, self.id, self.name, self.tpoll))
        return
    
    def init_from_bd( self, dcnf):
        '''
        self apunta a un objeto AnalogChannel
        '''
        CH = self.id
        self.name = dcnf.get((CH,'NAME'),'X')
        self.tpoll = float(dcnf.get((CH,'TPOLL'),0))
        return
    
    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.name == other.name and
            self.tpoll == other.tpoll ):
            return True
        else:
            return False

    def __ne__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.name == other.name == 'X' ):
            return False
        
        # Para el caso general
        if ( self.name != other.name or
            self.tpoll != other.tpoll):
            return True
        else:
            return False

    def get_response_string(self):
        response = '%s=%s,%s:' % (self.id, self.name, self.tpoll)
        return(response)
        
