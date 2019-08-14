# -*- coding: utf-8 -*-

'''
GET /cgi-bin/spx/SPY.pl?DLGID=TEST02&SIMPWD=DEFAULT&IMEI=860585004367917&VER=0.0.6.R1&UID=304632333433180f000500&SIMID=895980161423091055
&INIT&TPOLL=300&TDIAL=900&PWRS=ON,2330,0530&CSQ=25&WRST=0x33
&A0=A0,0,20,0.00,6.00&A1=A1,0,20,0.00,6.00&A2=A2,0,20,0.00,6.00&A3=A3,0,20,0.00,6.00&A4=A4,0,20,0.00,6.00
&D0=D0&D1=D1&C0=C0,0.10&C1=C1,0.10&CONS=OFF  HTTP/1.1
'''

import cgi
from datetime import datetime

from spy_bd_general import BD
from spy_bd_redis import Redis
from spy_set_base import Confbase
from spy_set_analog import Confanalog
from spy_set_digital import Confdigital
from spy_set_counter import Confcounter
from spy_set_doutput import Confdoutput

import logging
# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------

class INIT_frame:
   
    
    def __init__(self):
        # Inicializo guardando el query string y lo parseo
        # Todas las respuestas llevan el clock para resincronizar al datalogger.
        now = datetime.now()
        self.response = now.strftime('INIT_OK:CLOCK=%y%m%d%H%M:')
        
        form = cgi.FieldStorage()
        self.dlgid = form.getfirst('DLGID', 'DLG_ERR')    
        return
 
    
    def send_response(self):
        LOG.info('[%s] RSP=[%s]' % ( self.dlgid, self.response))
        print('Content-type: text/html\n')
        print('<html><body><h1>%s</h1></body></html>' % (self.response))
        return
 
         
    def process_frame(self):
        LOG.info('PV_process_frame')
        bd = BD()    
        # Leo toda la configuracion desde la BD en un dict.
        dcnf = bd.read_dlg_conf_from_bd( self.dlgid)
        if dcnf == {}:
            LOG.info('[%s] ERROR: No hay datos en la BD' % self.dlgid )
            self.response = 'ERROR'
            self.send_response()
            return
        
        # Proceso c/modulo
        self.PV_process_conf_parametros_base(dcnf)
        self.PV_process_conf_parametros_analog(dcnf)
        self.PV_process_conf_parametros_digital(dcnf)
        self.PV_process_conf_parametros_counter(dcnf)
        self.PV_process_conf_parametros_doutput(dcnf)
               
        # Creo un registo inicialiado en la redis.
        redis_db = Redis(self.dlgid)
        redis_db.create_rcd()
        
        self.send_response()
        return
    
    
    def PV_process_conf_parametros_base(self, dcnf):
        LOG.info('PV_process_conf_parametros_base')
        # Leo la configuracion base que trae el frame del datalogger
        self.confbase_dlg = Confbase(self.dlgid)        # Creo una conf.base vacia ( objeto )  
        self.confbase_dlg.init_from_qs()                # Lo relleno leyendo desde el query_string
        self.confbase_dlg.log(longformat=True, tag='dlgconf')

        # Creo una configuracion base que la leo de los datos de la BD.
        self.confbase_bd = Confbase(self.dlgid)
        self.confbase_bd.init_from_bd(dcnf)
        self.confbase_bd.log(longformat=False, tag='bdconf')
       
        if self.confbase_dlg == self.confbase_bd:
            LOG.info('[%s] Conf BASE: BD eq DLG' % (self.dlgid) )
        else:
            LOG.info('[%s] Conf BASE: BD ne DLG' % (self.dlgid) )
            self.response += self.confbase_bd.get_response_string( self.confbase_dlg )
            LOG.info('[%s] RSP=[%s]' % ( self.dlgid, self.response))
        # Actualizo la BD con datos de init ( ipaddress, imei, uid ....) que trae el datalogger
        self.confbase_dlg.update_bd()    
        return
    
    
    def PV_process_conf_parametros_analog(self, dcnf):
        LOG.info('PV_process_conf_parametros_analog') 
 
        self.confanalog_dlg = Confanalog(self.dlgid)    # Creo una conf.base vacia ( objeto )  
        self.confanalog_dlg.init_from_qs()              # Lo relleno leyendo desde el query_string
        self.confanalog_dlg.log(tag='dlgconf')

        # Creo una configuracion base que la leo de los datos de la BD.
        self.confanalog_bd = Confanalog(self.dlgid)
        self.confanalog_bd.init_from_bd(dcnf)
        self.confanalog_bd.log(tag='bdconf')
       
        if self.confanalog_dlg == self.confanalog_bd:
            LOG.info('[%s] Conf ANALOG: BD eq DLG' % (self.dlgid) )
        else:
            LOG.info('[%s] Conf ANALOG: BD ne DLG' % (self.dlgid) )
            self.response += self.confanalog_bd.get_response_string( self.confanalog_dlg )
            LOG.info('[%s] RSP=[%s]' % ( self.dlgid, self.response))
        return
 
       
    def PV_process_conf_parametros_digital(self, dcnf):
        LOG.info('PV_process_conf_parametros_digital')
                
        self.confdigital_dlg = Confdigital(self.dlgid)      
        self.confdigital_dlg.init_from_qs()              
        self.confdigital_dlg.log(tag='dlgconf')

        # Creo una configuracion base que la leo de los datos de la BD.
        self.confdigital_bd = Confdigital(self.dlgid)
        self.confdigital_bd.init_from_bd(dcnf)
        self.confdigital_bd.log(tag='bdconf')
       
        if self.confdigital_dlg == self.confdigital_bd:
            LOG.info('[%s] Conf DIGITAL: BD eq DLG' % (self.dlgid) )
        else:
            LOG.info('[%s] Conf DIGITAL: BD ne DLG' % (self.dlgid) )
            self.response += self.confdigital_bd.get_response_string( self.confdigital_dlg )
            LOG.info('[%s] RSP=[%s]' % ( self.dlgid, self.response))
        return

        
    def PV_process_conf_parametros_counter(self, dcnf):
        LOG.info('PV_process_conf_parametros_counter')
        
        self.confcounter_dlg = Confcounter(self.dlgid)      
        self.confcounter_dlg.init_from_qs()              
        self.confcounter_dlg.log(tag='dlgconf')

        # Creo una configuracion base que la leo de los datos de la BD.
        self.confcounter_bd = Confcounter(self.dlgid)
        self.confcounter_bd.init_from_bd(dcnf)
        self.confcounter_bd.log(tag='bdconf')
       
        if self.confcounter_dlg == self.confcounter_bd:
            LOG.info('[%s] Conf COUNTER: BD eq DLG' % (self.dlgid) )
        else:
            LOG.info('[%s] Conf COUNTER: BD ne DLG' % (self.dlgid) )
            self.response += self.confcounter_bd.get_response_string( self.confcounter_dlg )
            LOG.info('[%s] RSP=[%s]' % ( self.dlgid, self.response))
        return


    def PV_process_conf_parametros_doutput(self, dcnf):
        LOG.info('PV_process_conf_parametros_doutput')
        
        self.confdoutput_dlg = Confdoutput(self.dlgid)      
        self.confdoutput_dlg.init_from_qs()              
        self.confdoutput_dlg.log(tag='dlgconf')

        # Creo una configuracion base que la leo de los datos de la BD.
        self.confdoutput_bd = Confdoutput(self.dlgid)
        self.confdoutput_bd.init_from_bd(dcnf)
        self.confdoutput_bd.log(tag='bdconf')
       
        if self.confdoutput_dlg == self.confdoutput_bd:
            LOG.info('[%s] Conf DOUTPUT: BD eq DLG' % (self.dlgid) )
        else:
            LOG.info('[%s] Conf DOUTPUT: BD ne DLG' % (self.dlgid) )
            self.response += self.confdoutput_bd.get_response_string( self.confdoutput_dlg )
            LOG.info('[%s] RSP=[%s]' % ( self.dlgid, self.response))
        return
