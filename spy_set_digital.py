#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 08:04:51 2019

@author: pablo
"""
import logging
from spy_channel_digital import DigitalChannel

# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------

class Confdigital:
    '''
    Crea la estructura y metodos para manejar la configuracion de los canales
    digitales.
    Los dataloggers pueden tener hasta 8 canales ( SPX_IO5, SPX_IO8)
    '''
    
    def __init__(self, dlgid):
        self.dlgid = dlgid
        self.D0 = DigitalChannel('D0',dlgid)
        self.D1 = DigitalChannel('D1',dlgid)
        self.D2 = DigitalChannel('D2',dlgid)
        self.D3 = DigitalChannel('D3',dlgid)
        self.D4 = DigitalChannel('D4',dlgid)
        self.D5 = DigitalChannel('D5',dlgid)
        self.D6 = DigitalChannel('D6',dlgid)
        self.D7 = DigitalChannel('D7',dlgid)
        return
 
    
    def init_from_qs(self):
        '''
        Recibo un query string que lo parseo y relleno los campos del piloto
        '''
        self.D0.init_from_qs('D0')
        self.D1.init_from_qs('D1')
        self.D2.init_from_qs('D2')
        self.D3.init_from_qs('D3')
        self.D4.init_from_qs('D4')
        self.D5.init_from_qs('D5')
        self.D6.init_from_qs('D6')
        self.D7.init_from_qs('D7')
        return


    def log(self, tag='' ):
        self.D0.log(tag)
        self.D1.log(tag)
        self.D2.log(tag)
        self.D3.log(tag)
        self.D4.log(tag)
        self.D5.log(tag)
        self.D6.log(tag)
        self.D7.log(tag)
        return
 
              
    def init_from_bd( self, dcnf ):
        '''
        Leo la configuracion base del dlgid desde la base de datos y relleno la estructura self.
        La base de datos ya esta leida y los valores pasados en el dcnf.
        '''
        self.D0.init_from_bd(dcnf)
        self.D1.init_from_bd(dcnf)
        self.D2.init_from_bd(dcnf)
        self.D3.init_from_bd(dcnf)
        self.D4.init_from_bd(dcnf)
        self.D5.init_from_bd(dcnf)
        self.D6.init_from_bd(dcnf)
        self.D7.init_from_bd(dcnf)
        return


    def __eq__(self, other ):
        '''
        Overload de la comparacion donde solo comparo los elementos necesarios
        '''
        if ( self.D0 == other.D0 and
            self.D1 == other.D1 and
            self.D2 == other.D2 and
            self.D2 == other.D3 and
            self.D4 == other.D4 and
            self.D5 == other.D5 and
            self.D6 == other.D6 and
            self.D7 == other.D7 ):
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
        if self.D0 != other.D0:
            response += self.D0.get_response_string()
        if self.D1 != other.D1:
            response += self.D1.get_response_string()
        if self.D2 != other.D2:
            response += self.D2.get_response_string()
        if self.D3 != other.D3:
            response += self.D3.get_response_string()
        if self.D4 != other.D4:
            response += self.D4.get_response_string()
        if self.D5 != other.D5:
            response += self.D5.get_response_string()
        if self.D6 != other.D6:
            response += self.D6.get_response_string()
        if self.D7 != other.D7:
            response += self.D7.get_response_string()
            
        LOG.info('[%s] confdigital_RSP: [%s]' % ( self.dlgid,response))
        return(response)      

