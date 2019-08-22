#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 08:21:54 2019

@author: pablo
"""

import cgi
import re
from spy_log import log

#------------------------------------------------------------------------------
class CounterChannel():
    
    def __init__(self, ach_id, dlgid):
        self.dlgid = dlgid
        self.id = ach_id
        self.name = 'X'
        self.magpp = 0
        self.pwidth = 0
        self.period = 0
        self.speed = ''
        
    def init_from_qs ( self, ch_id ):
        '''
        qs es el string parseado tipo C0=CNT0,0.1,10,100,HS
        Primero vemos que no sea vacio.
        Luego lo parseamos y rellenamos los campos del self.
        C0,0.10,0.2,100,LS
        '''
        form = cgi.FieldStorage()
        field = form.getfirst(ch_id,'X,0,0,0,LS')    # Los canales que no estan los asumo apagados 'X'
        if field is not None:
            try:
                self.name, self.magpp, self.pwidth, self.period, self.speed, *r = re.split('=|,', field )
                self.magpp = float(self.magpp)
                self.pwidth = int(self.pwidth)
                self.period = int(self.period)
            except Exception as err_var:
                log(module=__name__, function='init_from_qs', level='INFO', dlgid=self.dlgid, msg='ERROR: {0}_unpack {1}'.format(ch_id, field))
                log(module=__name__, function='init_from_qs', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
        return
 
        
    def log ( self, tag = ''):
        log(module=__name__, function='log', level='SELECT', dlgid=self.dlgid, msg='{0} id={1}: name={2}, magpp={3}, pwidth={4}, period={5}, speed={6}'.format(tag,self.id, self.name, self.magpp, self.pwidth, self.period, self.speed))
        return
    
    def init_from_bd( self, dcnf):
        '''
        self apunta a un objeto AnalogChannel
        '''
        CH = self.id
        self.name = dcnf.get((CH,'NAME'),'X')
        self.magpp = float(dcnf.get((CH,'MAGPP'),0))
        self.pwidth = int(dcnf.get((CH,'PWIDTH'),0))
        self.period = int(dcnf.get(( CH,'PERIOD'),0))
        self.speed = dcnf.get((CH,'SPEED'),'LS')
        return
    
    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        # Para los casos en que no estan definidos los canales
        if ( self.name == other.name == 'X' ):
            return True
        
        # Para el caso general
        if ( self.name == other.name and
            self.magpp == other.magpp and
            self.pwidth == other.pwidth and
            self.period == other.period and
            self.speed == other.speed ):
            return True
        else:
            return False

    def __ne__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        # Para los casos en que no estan definidos los canales
        if ( self.name == other.name == 'X' ):
            return False

        # Para el caso general
        if ( self.name != other.name or
            self.magpp != other.magpp or
            self.pwidth != other.pwidth or
            self.period != other.period or
            self.speed != other.speed ):
            return True
        else:
            return False

    def get_response_string(self):
        response = '%s=%s,%s,%s,%s,%s:' % (self.id, self.name, self.magpp, int(self.pwidth), int(self.period),self.speed)
        return(response)
        
