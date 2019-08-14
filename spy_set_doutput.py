#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 09:49:58 2019

@author: pablo
"""

import cgi
import re
import logging

# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------

class Confdoutput:
    '''
    Crea la estructura y metodos para manejar la configuracion de las salidas 
    digitales.
    Pueden ser en modo CONSIGNA o PERFORACION o PILOTO.
    '''
    
    def __init__(self, dlgid):
        self.dlgid = dlgid
        self.douts = 'None'
        self.consigna_dia_hhmm = ''
        self.consigna_noche_hhmm = ''
        return
 
    def init_from_qs(self):
        '''
        qs es el string parseado tipo &DOUTS=CONS{PERF}{PLT}
        Primero vemos que no sea vacio.
        Luego lo parseamos y rellenamos los campos del self.
        Si el modo es CONS, entonces viene otra parametro que es CONS
        '''
        form = cgi.FieldStorage()
        self.douts = form.getfirst('DOUTS','')  
        if self.douts == 'CONS':
            try:
                self.consigna_dia_hhmm, self.consigna_noche_hhmm = re.split('=|,', form.getvalue('CONS'))   
                self.consigna_dia_hhmm = int( self.consigna_dia_hhmm)
                self.consigna_noche_hhmm = int( self.consigna_noche_hhmm)
            except:
                LOG.info('[%s] ERROR: douts_unpack [%s]' % ( self.dlgid, form.getvalue('DOUTS' )))
        return

    def log(self, tag='' ):
        LOG.info('[%s] %s douts=%s' % ( self.dlgid, tag, self.douts ))
        if self.douts == 'CONS':
            LOG.info('[%s] %s cons_dia_hhmm=%s' % ( self.dlgid, tag, self.consigna_dia_hhmm ))
            LOG.info('[%s] %s cons_noche_hhmm=%s' % ( self.dlgid, tag, self.consigna_noche_hhmm ))
        return
               
    def init_from_bd( self, dcnf ):
        '''
        Leo la configuracion base del dlgid desde la base de datos y relleno la estructura self.
        La base de datos ya esta leida y los valores pasados en el dcnf.
        '''
        self.douts = dcnf.get(('DOUTPUTS','MODO'))
        if self.douts == 'CONS':
            self.consigna_dia_hhmm = int(dcnf.get(('CONS','HHMM1'),0))
            self.consigna_noche_hhmm = int(dcnf.get(('CONS','HHMM2'),0))
        return

    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.douts == other.douts == 'CONS'):
            if ( self.consigna_dia_hhmm == other.consigna_dia_hhmm and
                self.consigna_noche_hhmm == other.consigna_noche_hhmm ):
                return True
            else:
                return False
        
        elif self.douts == other.douts:
            return True
        
        else:
            return False

    def get_response_string(self, other ):
        '''
        Genero el string que deberia mandarse al datalogger con la configuracion
        de los salidas digitales a reconfigurarse
        El objeto ( self) sobre el cual se llama debe ser la referencia de la BD.
        El other es el objeto con los datos del datalogger
        '''
        response = ''
                    
        if self.douts == 'CONS':
            response += 'DOUT=CONS:CSGNA=%s,%s:' % ( self.consigna_dia_hhmm, self.consigna_noche_hhmm )
         
        elif self.douts != other.douts:
            response += 'DOUT=%s:' % self.douts
                     
        LOG.info('[%s] confdputput_RSP: [%s]' % ( self.dlgid,response))
        return(response)      
