#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 21:50:46 2019

@author: pablo
"""

import cgi
import re
from spy_bd_gda import BDGDA
from spy import Config
from spy_log import log

#------------------------------------------------------------------------------

class Piloto:
    '''
    Esta clase define un piloto (su configuracion) y nos da los metodos
    para trabajar con el:
        - init
        - configurar sus parametros
        - imprimir los mismos
        - comparar ( la configuracion ) de dos 
    '''
    
    def __init__(self, dlgid = None):
        self.dlgid = dlgid
        self.pband = ''
        self.psteps = ''
        self.slot0 = ''
        self.slot1 = ''
        self.slot2 = ''
        self.slot3 = ''
        self.slot4 = ''
 
     
    def init_from_qs(self):
        '''
        Recibo un query string que lo parseo y relleno los campos del piloto
        '''
        form = cgi.FieldStorage()
        self.pband = float(form.getfirst('PBAND','0'))
        self.psteps = int(form.getfirst('PSTEPS','0'))
        self.slot0 = self.pv_parseSlotField( form.getfirst('S0', ('00', '1')))
        self.slot1 = self.pv_parseSlotField( form.getfirst('S1', ('01', '1')))
        self.slot2 = self.pv_parseSlotField( form.getfirst('S2', ('02', '1')))
        self.slot3 = self.pv_parseSlotField( form.getfirst('S3', ('03', '1')))
        self.slot4 = self.pv_parseSlotField( form.getfirst('S4', ('04', '1')))
        return
  
        
    def init_from_bd( self, dlgid ):
        '''
        Leo la configuracion de pilotos del dlgid y relleno la estructura self.
        '''
        bd = BDGDA( Config['MODO']['modo'] )
         # Leo la configuracion en un dict.
        d = bd.read_conf_piloto(dlgid)
        # Controlo que hallan datos.
        if d == {}:
            log(module=__name__, function='init_from_bd', dlgid=dlgid, msg='ERROR: No hay datos en la bd')
            return False
        
        # Convierto el dict a la estructura Piloto
        self.dlgid = dlgid
        self.pband = float(d.get('PBAND', '0.2'))
        self.psteps = int(d.get('PSTEPS','6'))
        self.slot0 = ( int(d.get('HHMM_0','00')), float(d.get('POUT_0','1')))
        self.slot1 = ( int(d.get('HHMM_1','00')), float(d.get('POUT_1','1')))
        self.slot2 = ( int(d.get('HHMM_2','00')), float(d.get('POUT_2','1')))
        self.slot3 = ( int(d.get('HHMM_3','00')), float(d.get('POUT_3','1')))
        self.slot4 = ( int(d.get('HHMM_4','00')), float(d.get('POUT_4','1')))
        return True
  
      
    def __str__(self):
        response = 'PILOTO: dlgid=%s, pband=%s, psteps=%s ' % ( self.dlgid, self.pband, self.psteps )
        response += 'S0=%s,%s ' % ( self.slot0[0], self.slot0[1])
        response += 'S1=%s,%s ' % ( self.slot1[0], self.slot1[1])
        response += 'S2=%s,%s ' % ( self.slot2[0], self.slot2[1])
        response += 'S3=%s,%s ' % ( self.slot3[0], self.slot3[1])
        response += 'S4=%s,%s' % ( self.slot4[0], self.slot4[1])  
        return(response)
 
       
    def __eq__(self, other ):
        if ( self.pband == other.pband and
            self.psteps == other.psteps and
            self.slot0 == other.slot0 and
            self.slot1 == other.slot1 and
            self.slot2 == other.slot2 and
            self.slot3 == other.slot3 and
            self.slot4 == other.slot4 ):
            return True
        else:
            return False

          
    def get_response_string(self):
        '''
        Genero el string que deberia mandarse al datalogger con la configuracion
        en la BD del piloto para ese dlgid
        '''
        response = 'PLT_OK&PBAND=%s&PSTEPS=%s' % (self.pband, self.psteps )
        response += '&S0=%s,%s' % (self.slot0[0], self.slot0[1])
        response += '&S1=%s,%s' % (self.slot1[0], self.slot1[1])
        response += '&S2=%s,%s' % (self.slot2[0], self.slot2[1])
        response += '&S3=%s,%s' % (self.slot3[0], self.slot3[1])
        response += '&S4=%s,%s' % (self.slot4[0], self.slot4[1])
        return(response)      
  
               
    def pv_parseSlotField(self, sline ):
        '''
        Funcion privada para parsear las lineas con los datos de hhmm y pout
        de c/slot de trabajo de los pilotos
        '''
        # Los datos de c/slot vienen como hhmm,pout. Los parseo y los
        # retorno como una tupla
        (hhmm, pout ) = re.split( '=|,', sline)
        return( int(hhmm), float(pout))


#------------------------------------------------------------------------------
 