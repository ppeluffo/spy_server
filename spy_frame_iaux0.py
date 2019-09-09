# -*- coding: utf-8 -*-

"""
Protocolo:
Se introduce el frame de pilotos IAUX0 a partir de R 2.0.5 @ 2019-07-16

A partir de esta version, el modo de trabajo de las salidas viene en el parametro
DOUTPUTS en el frame de init_A, y si estas son del tipo PLT, luego viene otro
frame de init_B donde viene la configuracion del piloto

Sent:
GET /cgi-bin/PY/spy.py?IAUX0&DLGID=TEST02&PBAND=0.2&PSTEPS=6&S0=hhmm0,p0&S1=hhmm1,p1&S2=hhmm2,p2&S3=hhmm3,p3&S4=hhmm4,p4 HTTP/1.1
Host: www.spymovil.com

Receive:
   PLT_OK  Si la conf. de la BD y la del dlg son iguales
   PLT_OK&PBAND=a&PSTEPS=b&S0=1,2&.....&S4=3,2.3 Si son diferentes
    
Procedimiento:

    Leo la configuracion del datalogger y la de la BD.
    Las comparo y genero la respuesta de acuerdo al protocolo

  
Testing:
- Con telnet:
telnet localhost 80
GET /cgi-bin/PY/spy.py?IAUX0&DLGID=TEST01&PBAND=0.2&PSTEPS=6&S0=230,1.3&S1=450,2.1&S2=1050,2.1&S3=1735,1.6&S4=2350,0.0 HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/PY/spy.py?IAUX0&DLGID=TEST01&PBAND=0.2&PSTEPS=6&S0=230,1.2&S1=430,2.4&S2=650,1.8&S3=1040,1.6&S4=1750,1.3 HTTP/1.1
Host: www.spymovil.com

- Con browser:
> usamos el url: http://localhost/cgi-bin/PY/spy.py?IAUX0&DLGID=TEST01&PBAND=0.2&PSTEPS=6&S0=230,1.3&S1=450,2.1&S2=1050,2.1&S3=1735,1.6&S4=2350,0.0


"""

import cgi
from spy_channel_piloto import Piloto
from spy_log import log
from spy import Config
from spy_bd_general import BD

#------------------------------------------------------------------------------
class IAUX0_frame:
  
    
    def __init__(self ):
        # Inicializo guardando el query string y lo parseo
        self.response = ''
        form = cgi.FieldStorage()
        self.dlgid = form.getfirst('DLGID', 'DLG_ERR')
        return
    
    
    def send_response(self ):
        log(module=__name__, function='send_response', dlgid=self.dlgid, msg='RSP={0}'.format(self.response))
        print('Content-type: text/html\n')
        print('<html><body><h1>%s</h1></body></html>' % (self.response))
   
       
    def process(self):
        '''
        Defino la logica de procesar los frames iaux0.
        Por ahora solo traen la configuracion del piloto.
        En el init, carque la configuracion que envia el datalogger en el frame.
        Creo una estructura Piloto generica y leo de la bd los parametros que tiene
        configurados dicho datalogger.
        Compara las estructuras del dlg y la bd y genero la respuesta en consecuencia
        Para esto hice un overload de metodo __eq__ invocado por el operador ==.
        Tambien hice un overload del metodo __str__ usado por el print.
        '''
        log(module=__name__, function='process', dlgid=self.dlgid, level='SELECT', msg='start' )
        # Leo toda la configuracion desde la BD en un dict.
        bd = BD( modo = Config['MODO']['modo'], dlgid = self.dlgid )
        dplt = bd.read_piloto_conf()
        if dplt == {}:
            log(module=__name__, function='process', dlgid=self.dlgid, msg='ERROR: No hay datos en la BD')
            self.response = 'SRV_ERR'
            self.send_response()
            return

        self.process_conf_piloto(dplt)
        self.send_response()
        return


    def process_conf_piloto(self,dplt):
        log(module=__name__, function='process_conf_piloto', dlgid=self.dlgid, level='SELECT', msg='start')

        self.plt_dlg = Piloto(self.dlgid)
        self.plt_dlg.init_from_qs()
        self.plt_dlg.log(tag='dlgconf')

        self.plt_bd = Piloto(self.dlgid)
        self.plt_bd.init_from_bd(dplt)
        self.plt_bd.log(tag='bdconf')

        # Comparo la configuracion que trae el dlg y la de la bd y repondo al datalogger
        response = ''
        if self.plt_dlg == self.plt_bd:
            log(module=__name__, function='process_conf_piloto', dlgid=self.dlgid, level='SELECT',msg='Conf PLT: BD eq DLG')
            self.response = 'PLT_OK'
        else:
            log(module=__name__, function='process_conf_piloto', dlgid=self.dlgid, level='SELECT',msg='Conf PLT: BD ne DLG')
            self.response = self.plt_bd.get_response_string()
        return

    
    
    
    
    
    
    
    
    
    
    
    