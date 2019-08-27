# -*- coding: utf-8 -*-
"""
Los frames de datos son del tipo:
GET /cgi-bin/spx/spy.pl?DLGID=SPY001&PASSWD=spymovil123&VER=3.0.0&
&CTL=25&LINE=20000101,001555,pA=-74.85,pB=-493.20,pC=-964.54,Q1=0.0,V2=0.0,L1=0,bt=12.70
&CTL=26&LINE=20000101,002056,pA=-74.86,pB=-493.00,pC=-964.71,Q1=0.0,V2=0.0,L1=0,bt=12.72
&CTL=27&LINE=20000101,002639,pA=-74.90,pB=-492.70,pC=-964.51,Q1=0.0,V2=0.0,L1=0,bt=12.73
&CTL=28&LINE=20000101,003140,pA=-74.88,pB=-492.86,pC=-965.01,Q1=0.0,V2=0.0,L1=0,bt=12.75
&CTL=29&LINE=20000101,003641,pA=-74.82,pB=-492.96,pC=-964.99,Q1=0.0,V2=0.0,L1=0,bt=12.72
CTL=30&LINE=20000101,004142,pA=-74.83,pB=-492.97,pC=-964.51,Q1=0.0,V2=0.0,L1=0,bt=12.72  HTTP/1.1
Host: www.spymovil.com
    
Pueden venir varios.
Luego de c/u, el servidor responde con el id de control del ultimo a modo de ACK.
En este caso seria 30.

"""

import cgi
import os
from spy import Config
from spy_bd_redis import Redis
from spy_bd_general import BD
from datetime import datetime
from spy_log import log

#------------------------------------------------------------------------------

class DATA_frame:


    def __init__(self):
        form = cgi.FieldStorage()
        self.dlgid = form.getfirst('DLGID', 'DLG_ERR')
        # ojo: tengo varios campos con la misma key: CTL, LINE
        # Dejo las lineas en una lista
        self.data_lines = form.getlist('LINE')  # Retorna [] si no existe la key
        # y los control en otra
        self.control_codes = form.getlist('CTL')
        self.response = 'RX_OK:'
        log(module=__name__, function='__init__', dlgid=self.dlgid, msg='start')
        return


    def send_response(self):
        log(module=__name__, function='send_response', dlgid=self.dlgid, msg='RSP={0}'.format(self.response))
        print('Content-type: text/html\n')
        print('<html><body><h1>%s</h1></body></html>' % (self.response))
        return


    def process_commited_conf(self):
        '''
        Leo de la BD el commited_conf. Si esta en 1 lo pongo en 0
        Si en la respuesta no hay un reset ( puesto por la REDIS ) y el
        commited_conf fue 1, pongo un RESET.
        '''
        log(module=__name__, function='process_commited_conf', dlgid=self.dlgid, level='SELECT', msg='start')
        bd = BD( modo = Config['MODO']['modo'], dlgid=self.dlgid )
        commited_conf = bd.process_commited_conf()
        if ( (commited_conf == 1) and ( 'RESET' not in self.response) ):
            self.response += 'RESET:'
            bd.clear_commited_conf()


    def process(self):
        '''
        Proceso un frame de datos.
        Consiste en escribir el payload self.data_lines en un archivo con el 
        nombre del dlgid y la fecha del momento.
        Retorno a modo de ACK el codigo de control de la ultima linea
        - La ultima linea la debo guardar en la redis
        - Veo si en la redis hay comandos para mandarle al datalogger
        - Chequeo los callbacks ( No por ahora )
        '''
        now = datetime.now()
        rxpath = Config['PATH']['rx_path']
        tmp_fname = '%s_%s.tmp' % ( self.dlgid, now.strftime('%Y%m%d%H%M%S') )
        tmp_file = os.path.join( os.path.abspath(''), rxpath, tmp_fname )
        dat_fname = '%s_%s.dat' % ( self.dlgid, now.strftime('%Y%m%d%H%M%S') )
        dat_file = os.path.join( os.path.abspath(''), rxpath, dat_fname )
        log(module=__name__, function='process', dlgid=self.dlgid, level='SELECT', msg='FILE: {}'.format(dat_file))
        # Abro un archivo temporal donde guardo los datos
        ziplines = list(zip(self.control_codes, self.data_lines))
        with open(tmp_file, 'w') as fh:
            for (control_code, data_line) in ziplines:
                fh.write(data_line + '\n')
                log(module=__name__, function='process', dlgid=self.dlgid, level='SELECT',msg='clt={0}, datline={1}'.format(control_code, data_line))
        # Al final lo renombro.
        os.rename(tmp_file, dat_file)
        # Genero la respuesta
        frame_id = self.control_codes[-1]
        self.response += '%s:' % frame_id   
        
        # Actualizo la redis y leo si hay algo para mandar al datalogger
        redis_db = Redis(self.dlgid)
        # Guardo la ultima linea en la redis
        redis_db.insert_line( self.data_lines[-1] )
        # Si hay comandos en la redis los incorporo a la respuesta
        self.response += redis_db.get_cmd_outputs()
        self.response += redis_db.get_cmd_pilotos()
        self.response += redis_db.get_cmd_reset()

        self.process_commited_conf()

        # Envio la respuesta
        self.send_response()
        return
