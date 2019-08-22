# -*- coding: utf-8 -*-

"""
Protocolo:
Se introduce el fram de SCAN a partir de R 1.0.4 @ 2019-05-10

Sent:
GET /cgi-bin/PY/spy.py?SCAN&DLGID=dlg&UID=abcd45367 HTTP/1.1
Host: www.spymovil.com

Receive:
   SCAN_ERR
   SCAN_OK
   DLGID=$dlgid
   NOTDEFINED
    
Procedimiento:
        Vemos que el DLGID este definido en la BD en la tabla 'spy_equipo'
        Si esta definido actualizo el UID en la BD.spy_equipo
        Retorno: SCAN_OK
        
        Si el DLGID no esta definido pero si el UID, leo el dlgid que corresponde
        y se lo mando.
        Retorno: DLGID=$dlgid
        
        Si no tengo definido ni el UID ni el DLGID retorno error
        Retorno: NOTDEFINED
        
        En caso de problemas, retorno SCAN_ERR
  
Testing:
- Con telnet:
telnet localhost 80
GET /cgi-bin/PY/spy.py?SCAN&DLGID=dlg&UID=abcd45367 HTTP/1.1
Host: www.spymovil.com

- Con browser:
> usamos el url: http://localhost/cgi-bin/PY/spy.py?SCAN&DLGID=TEST01&UID=abcd45367


"""

import cgi
from spy_bd_bdspy import BDSPY
from spy import Config
from spy_log import log

#------------------------------------------------------------------------------
class SCAN_frame:
   
    def __init__(self ):
        # Parseo el query_string y genero un objeto 'DNI'
        form = cgi.FieldStorage()   
        self.dlgid = form.getvalue('DLGID')
        self.uid = form.getvalue('UID')
        log(module=__name__, function='__init__', dlgid=self.dlgid, msg='DLGID={0}, UID={1}'.format ( self.dlgid, self.dlgid, self.uid))
        return
    
    def send_response(self, response ):
        log(module='SCAN', function='send_response', dlgid=self.dlgid, msg='RSP={0}'.format(response))
        print('Content-type: text/html\n')
        print('<html><body><h1>%s</h1></body></html>' % (response) )
   
    def process(self):
        '''
        Define la logica de procesar los frames de SCAN
        Primero hago una conexion a la BDSPY.
        - Si el dlgid esta definido, OK
        - Si no esta pero si el uid, le mando al datalogger que se reconfigure
           con el dlg asociado al uid.
        - Si no esta el dlgid ni el uid, ERROR
        '''
        
        bd = BDSPY( Config['MODO']['modo'] )
        # Primero vemos que el dlgid este definido sin importar su uid
        if bd.dlg_is_defined( self.dlgid ):
            log(module=__name__, function='process', dlgid=self.dlgid, msg='SCAN OK')
            self.send_response('SCAN_OK')
        # Si no esta definido, busco si hay algun uid y a que dlgid corresponde
        elif bd.uid_is_defined( self.uid ):
            new_dlgid = bd.get_dlgid_from_uid( self.uid )
            log(module=__name__, function='process', dlgid=self.dlgid, msg='bdconf NEW_DLGID={}'.format(new_dlgid))
            self.send_response('DLGID=%s' % new_dlgid )
        else:
            log(module=__name__, function='process', dlgid=self.dlgid, msg='DLGID/UID not Defined !!')
            self.send_response('NOTDEFINED')
            
        return
    

        
    