# -*- coding: utf-8 -*-
"""
Modulo de conexion a la BD.
Implemento la BD como una clase
En el init me conecto y me quedo con el conector.
Luego, c/metodo me da los parametros que quiera
"""

from spy_bd_gda import BDGDA
from spy_bd_ose import BDOSE
from spy_bd_bdspy import BDSPY
from spy_log import log
#------------------------------------------------------------------------------
class BD:
    
    def __init__ (self, modo = 'local'):
         self.datasource = ''
         self.modo = modo
         return


    def process_commited_conf(self, dlgid):

        self.find_datasource(dlgid)
        log(module=__name__, function='process_commited_conf', level='SELECT', dlgid=dlgid, msg='DS={}'.format(self.datasource))

        if self.datasource == 'GDA':
            bd = BDGDA(self.modo)
        elif self.datasource == 'bd_ose':
            bd = BDOSE(self.modo)
        else:
            log(module=__name__, function='process_commited_conf', level='INFO', dlgid=dlgid, msg='DS={} NOT implemented'.format(self.datasource))
            return

        cc = bd.process_commited_conf(dlgid)
        return (cc)


    def clear_commited_conf(self, dlgid):
        '''
        Reseteo el valor de commited_conf a 0
        '''
        self.find_datasource(dlgid)
        log(module=__name__, function='clear_commited_conf', level='SELECT', dlgid=dlgid, msg='DS={}'.format(self.datasource))

        if self.datasource == 'GDA':
            bd = BDGDA(self.modo)
        elif self.datasource == 'bd_ose':
            bd = BDOSE(self.modo)
        else:
            log(module=__name__, function='clear_commited_conf', dlgid=dlgid, msg='DS ERROR !!' )
            return

        cc = bd.clear_commited_conf(dlgid)
        return


    def find_datasource( self, dlgid ):
        """
        Determina en que base de datos (GDA/TAHONA/bd_ose) esta definido el dlg
        Retorna True/False
        En la bd_ose tenemos 3 grupos de tablas: PZ(pozos), TQ(tanques), PQ(presiones y caudales)
        En esta instancia si el datasource es bd_ose, determinamos a que grupo pertenece de modo
        de ahorrarle trabajo luego a las funciones de lectura/escritura de la BD.
        """
        # Si ya tengo el datasource no tengo que consultar a la BD.
        if self.datasource == 'GDA' or self.datasource == 'bd_ose':
            return

        bd = BDSPY(self.modo)
        self.datasource = bd.find_data_source(dlgid)
        return


    def reset_datasource(self, dlgid):
        self.datasource = ''
        log(module=__name__, function='reset_datasource', level='SELECT', dlgid=dlgid, msg='start')


    def read_dlg_conf( self, dlgid):
        '''
        Leo toda la configuracion de la BD que hay del dlg dado.
        El dlg puede estar en cualquier BD ( gda, bd_ose ) y con esta funcion abstraigo al resto
        del programa de donde esta.
        Retorno un diccionario con un indice doble ( canal, parametro )
        '''
        log(module=__name__, function='read_dlg_conf', level='SELECT', dlgid=dlgid, msg='start')
        # En ppio no se en que BD esta definido el datalogger. Debo leer su datasource

        self.find_datasource(dlgid)
        log(module=__name__, function='read_dlg_conf', level='SELECT', dlgid=dlgid, msg='DS={}'.format(self.datasource))

        if self.datasource == 'GDA':
            bd = BDGDA(self.modo)
        elif self.datasource == 'bd_ose':
            bd = BDOSE(self.modo)
        else:
            log(module=__name__, function='read_dlg_conf', dlgid=dlgid,msg='DS ERROR !!')
            return

        d = bd.read_dlg_conf(dlgid)
        return (d)


    def update(self, dlgid, d):
        '''
        Recibo un diccionario y a partir de este genero los inserts en la BD.
        Determino en que BD debo trabajar.

        '''
        self.find_datasource(dlgid)
        log(module=__name__, function='update', level='SELECT', dlgid=dlgid, msg='DS={}'.format(self.datasource))

        if self.datasource == 'GDA':
            bd = BDGDA(self.modo)
        elif self.datasource == 'bd_ose':
            bd = BDOSE(self.modo)
        else:
            log(module=__name__, function='update', level='SELECT', dlgid=dlgid, msg='DS ERROR !!')
            return

        bd.update(dlgid, d)
        return


    def insert_data_line(self, dlgid, d):

        self.find_datasource(dlgid)
        log(module=__name__, function='insert_data_line', level='SELECT', dlgid=dlgid, msg='DS={}'.format(self.datasource))

        if self.datasource == 'GDA':
            bd = BDGDA(self.modo)
        elif self.datasource == 'bd_ose':
            bd = BDOSE(self.modo)
        else:
            log(module=__name__, function='insert_data_line', dlgid=dlgid, msg='DS ERROR !!' )
            return

        bd.insert_data_line(dlgid, d)
        bd.insert_data_online(dlgid, d)
        return

