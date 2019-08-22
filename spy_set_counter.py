#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 08:21:50 2019

@author: pablo
"""

import logging
from spy_channel_counter import CounterChannel
from spy_log import log

#------------------------------------------------------------------------------

class Confcounter:
    '''
    Crea la estructura y metodos para manejar la configuracion de los canales
    digitales.
    Los dataloggers tienen solo 2 contadores
    '''
    
    def __init__(self, dlgid):
        self.dlgid = dlgid
        self.C0 = CounterChannel('C0',dlgid)
        self.C1 = CounterChannel('C1',dlgid)
        return
 
    def init_from_qs(self):
        '''
        Recibo un query string que lo parseo y relleno los campos del piloto
        '''
        self.C0.init_from_qs('C0')
        self.C1.init_from_qs('C1')
        return

    def log(self, tag='' ):
        self.C0.log(tag)
        self.C1.log(tag)
        return
               
    def init_from_bd( self, dcnf ):
        '''
        Leo la configuracion base del dlgid desde la base de datos y relleno la estructura self.
        La base de datos ya esta leida y los valores pasados en el dcnf.
        '''
        self.C0.init_from_bd(dcnf)
        self.C1.init_from_bd(dcnf)
        return

    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.C0 == other.C0 and
            self.C1 == other.C1 ):
            return True
        else:
            return False

    def get_response_string(self, other ):
        '''
        Genero el string que deberia mandarse al datalogger con la configuracion
        de los canales analogicos a reconfigurarse
        El objeto ( self) sobre el cual se llama debe ser la referencia de la BD.
        El other es el objeto con los datos del datalogger
        '''
        response = ''
        if self.C0 != other.C0:
            response += self.C0.get_response_string()
        if self.C1 != other.C1:
            response += self.C1.get_response_string()

        log(module=__name__, function='get_response_string', level='SELECT', dlgid=self.dlgid, msg='confbase_RSP: {}'.format(response))
        return(response)

