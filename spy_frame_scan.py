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
import logging
from spy_bd_general import BD

# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------
class SCAN_frame:
   
    def __init__(self ):
        # Parseo el query_string y genero un objeto 'DNI'
        form = cgi.FieldStorage()   
        self.dlgid = form.getvalue('DLGID')
        self.uid = form.getvalue('UID')
        LOG.info('[%s] dlgconf DLGID=%s, UID=%s' % ( self.dlgid, self.dlgid, self.uid))
        return
    
    def sendResponse(self, response ):
        LOG.info('[%s] RSP=[%s]' % ( self.dlgid, response) )
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
        
        bd = BD()
        # Primero vemos que el dlgid este definido sin importar su uid
        if bd.dlgIsDefined( self.dlgid ): 
            LOG.info('[%s] Scan OK !'% ( self.dlgid) )
            self.sendResponse('SCAN_OK')     
        # Si no esta definido, busco si hay algun uid y a que dlgid corresponde
        elif bd.uidIsDefined( self.uid ):
            new_dlgid = bd.get_dlgid_from_uid( self.uid )
            LOG.info('[%s] bdconf NEW_DLGID=%s' % ( self.dlgid, new_dlgid) )
            self.sendResponse('DLGID=%s' % new_dlgid )      
        else:
            LOG.info('[%s] DLGID/UID not Defined !!'% ( self.dlgid) )
            self.sendResponse('NOTDEFINED')
            
        return
    

        
    