#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 20:51:49 2019

@author: pablo
"""

from spy_channel_analog import AnalogChannel
from spy_log import log

#------------------------------------------------------------------------------

class Confanalog:
    '''
    Crea la estructura y metodos para manejar la configuracion de los canales
    analogicos.
    Los dataloggers pueden tener hasta 8 canales ( SPX_IO5, SPX_IO8)
    '''
    
    def __init__(self, dlgid):
        self.dlgid = dlgid
        self.A0 = AnalogChannel('A0',dlgid)
        self.A1 = AnalogChannel('A1',dlgid)
        self.A2 = AnalogChannel('A2',dlgid)
        self.A3 = AnalogChannel('A3',dlgid)
        self.A4 = AnalogChannel('A4',dlgid)
        self.A5 = AnalogChannel('A5',dlgid)
        self.A6 = AnalogChannel('A6',dlgid)
        self.A7 = AnalogChannel('A7',dlgid)
        return
 
    
    def init_from_qs(self):
        '''
        Recibo un query string que lo parseo y relleno los campos del piloto
        '''
        self.A0.init_from_qs('A0')
        self.A1.init_from_qs('A1')
        self.A2.init_from_qs('A2')
        self.A3.init_from_qs('A3')
        self.A4.init_from_qs('A4')
        self.A5.init_from_qs('A5')
        self.A6.init_from_qs('A6')
        self.A7.init_from_qs('A7')
        return


    def log(self, tag='' ):
        self.A0.log(tag)
        self.A1.log(tag)
        self.A2.log(tag)
        self.A3.log(tag)
        self.A4.log(tag)
        self.A5.log(tag)
        self.A6.log(tag)
        self.A7.log(tag)
        return
  
             
    def init_from_bd( self, dcnf ):
        '''
        Leo la configuracion base del dlgid desde la base de datos y relleno la estructura self.
        La base de datos ya esta leida y los valores pasados en el dcnf.
        '''
        self.A0.init_from_bd(dcnf)
        self.A1.init_from_bd(dcnf)
        self.A2.init_from_bd(dcnf)
        self.A3.init_from_bd(dcnf)
        self.A4.init_from_bd(dcnf)
        self.A5.init_from_bd(dcnf)
        self.A6.init_from_bd(dcnf)
        self.A7.init_from_bd(dcnf)
        return


    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.A0 == other.A0 and
            self.A1 == other.A1 and
            self.A2 == other.A2 and
            self.A2 == other.A3 and
            self.A4 == other.A4 and
            self.A5 == other.A5 and
            self.A6 == other.A6 and
            self.A7 == other.A7 ):
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
        if self.A0 != other.A0:
            response += self.A0.get_response_string()
        if self.A1 != other.A1:
            response += self.A1.get_response_string()
        if self.A2 != other.A2:
            response += self.A2.get_response_string()
        if self.A3 != other.A3:
            response += self.A3.get_response_string()
        if self.A4 != other.A4:
            response += self.A4.get_response_string()
        if self.A5 != other.A5:
            response += self.A5.get_response_string()
        if self.A6 != other.A6:
            response += self.A6.get_response_string()
        if self.A7 != other.A7:
            response += self.A7.get_response_string()

        log(module=__name__, function='get_response_string', level='SELECT', dlgid=self.dlgid, msg='confanalog_RSP: {}'.format(response))
        return(response)
