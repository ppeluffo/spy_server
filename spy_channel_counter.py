#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 08:21:54 2019

@author: pablo
"""

import cgi
import re
import logging

# Creo un logger local child.
LOG = logging.getLogger(__name__)

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
            except:
                LOG.info('[%s] ERROR: %s_unpack [%s]' % ( self.dlgid, ch_id, field ))
        return
 
        
    def log ( self, tag = ''):
        LOG.info('[%s] %s id=%s: name=%s, magpp=%s, pwidth=%s, period=%s, speed=%s' % ( self.dlgid, tag, self.id, self.name, self.magpp, self.pwidth, self.period, self.speed ))
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
        
