#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 12:03:49 2019

@author: pablo
"""

GET /cgi-bin/PY/spy.py?DATA&DLGID=DLGID=TEST09&UID=304632333433180f000500 HTTP/1.1
Host: www.spymovil.com

             bd = BD()
        if not bd.connect_bdspy():
            self.sendResponse('SCAN BD_ERROR')
            return
        
        if bd.dlgIsDefined(self.dlgid):        
            self.sendResponse('SCAN_OK')
            
        elif bd.uidIsDefined( self.uid ):
            new_dlgid = bd.getDlgid()
            self.sendResponse('DLGID=%s' % new_dlgid )
            
        else:
   
    my $query = "SELECT spx_unidades_configuracion.nombre as \"canal\", spx_configuracion_parametros.parametro, 
	spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
	spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
	WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
	AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
	AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
	AND spx_unidades_configuracion.nombre = \"PILOTO\"
	AND spx_unidades.dlgid = \"$DLGID\"";
          
    
    
          d = dict()
        for row in results:
            tag, pname, value, id = row
            d[pname] = value
            
        return(d)



        print(query)
        rp = self.conn_bdspy.execute(query)
        results = rp.fetchall()
        if len(results) == 0:
            return False



        print('Content-type: text/html\n')
        print('<html><body>')
        for key in sorted (bdrec.keys()):
            value = bdrec[key]
            print('<h1>%s=%s</h1>' % (key,value))
        print('</html></body>')    

        if self.checkConfig() is True:
            response = '&PBAND=%d&PSTEPS=%s' % ( self.bdrec['pband'], self.bdrec['psteps'] )
            """
            response += '&S0=%s,%s' % self.bdrec['hhmm_0'], self.bdrec['pout_0']
            response += '&S1=%s,%s' % self.bdrec['hhmm_1'], self.bdrec['pout_1']
            response += '&S2=%s,%s' % self.bdrec['hhmm_2'], self.bdrec['pout_2']
            response += '&S3=%s,%s' % self.bdrec['hhmm_3'], self.bdrec['pout_3']
            response += '&S4=%s,%s' % self.bdrec['hhmm_4'], self.bdrec['pout_4']
            """


   def checkConfig(self):

        ret = True
        for key in sorted (self.dlgrec.keys()):
            dlg_value = self.dlgrec[key]
            bd_value = self.bdrec[key]
            if dlg_value != bd_value:
                ret = False
        return(ret)


       # Si son diferetes armo el return string
        
        print('Content-type: text/html\n')
        print('<html><body>')
        #for key in sorted (d.keys()):
        #    value = d[key]
        #    print('<h1>%s=%s</h1>' % (key,value))
        for key in self.bdplt._fields:
            value = getattr(self.bdplt, key)
            print('<h1>%s=%s</h1>' % (key,value))
            
        print('</html></body>')        
        #response = 'PLT_OK'

http://localhost/cgi-bin/PY/spy.py?IAUX0&DLGID=TEST01&PBAND=0.2&PSTEPS=6&S0=230,1.3&S1=450,2.1&S2=1050,2.1&S3=1735,1.6&S4=2350,0.2
http://localhost/cgi-bin/PY/spy.py?IAUX0&DLGID=TEST01&PBAND=0.2&PSTEPS=6&S0=230,1.3&S1=450,2.1&S2=1050,2.1&S3=1735,1.6&S4=2350,0.2
