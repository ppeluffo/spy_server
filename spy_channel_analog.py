#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 21:12:58 2019

@author: pablo
"""
import cgi
import re
import logging

# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------
class AnalogChannel():
  
    
    def __init__(self, ach_id, dlgid):
        self.dlgid = dlgid
        self.id = ach_id
        self.name = 'X'
        self.imin = 0
        self.imax = 0
        self.mmin = 0
        self.mmax = 0
 
       
    def init_from_qs ( self, ch_id ):
        '''
        qs es el string parseado tipo A1=PA,0,20,0.00,6.00
        Primero vemos que no sea vacio.
        Luego lo parseamos y rellenamos los campos del self.
        '''
        form = cgi.FieldStorage()
        field = form.getfirst(ch_id,'X,0,0,0,0')    # Los canales que no estan los asumo apagados 'X'
        if field is not None:
            try:
                self.name, self.imin, self.imax, self.mmin, self.mmax = re.split('=|,', field ) 
                self.imin = float(self.imin)
                self.imax = float(self.imax)
                self.mmin = float(self.mmin)
                self.mmax = float(self.mmax)
            except:
                LOG.info('[%s] ERROR: %s_unpack [%s]' % ( self.dlgid, ch_id, field ))
        return
 
    
    def log ( self, tag = ''):
        LOG.info('[%s] %s id=%s: name=%s,imin=%s,imax=%s,mmin=%s,mmax=%s' % ( self.dlgid, tag, self.id, self.name, self.imin, self.imax, self.mmin, self.mmax ))
        return
  
    
    def init_from_bd( self, dcnf):
        '''
        self apunta a un objeto AnalogChannel
        Por defecto si el canal no esta definido lo tomo como 'X'
        '''
        CH = self.id
        self.name = dcnf.get((CH,'NAME'),'X')
        self.imin = float(dcnf.get((CH,'IMIN'),4))
        self.imax = float(dcnf.get((CH,'IMAX'),20))
        self.mmin = float(dcnf.get((CH,'MMIN'),0))
        self.mmax = float(dcnf.get((CH,'MMAX'),10))
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
            self.imin == other.imin and
            self.imax == other.imax and
            self.mmin == other.mmin and
            self.mmax == other.mmax ):
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
            self.imin != other.imin or
            self.imax != other.imax or
            self.mmin != other.mmin or
            self.mmax != other.mmax ):
            return True
        else:
            return False


    def get_response_string(self):
        response = '%s=%s,%s,%s,%s,%s:' % (self.id, self.name, int(self.imin), int(self.imax) ,self.mmin,self.mmax)
        return(response)
        