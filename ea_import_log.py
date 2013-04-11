# -*- coding: utf-8 -*-
##############################################################################
#
#    Copyright (C) 2011 Enapps LTD (<http://www.enapps.co.uk>).
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

from osv import osv
from osv import fields


class ea_import_log(osv.osv):
    _name = 'ea_import.log'
    _order = 'import_time desc'
    _columns = {
        'log_line': fields.one2many('ea_import.log.line', 'log_id', 'Log Line'),
        'import_time': fields.datetime('Import Time',),
        'chain_id': fields.many2one('ea_import.chain', 'Import Chain'),
        'result_ids': fields.one2many('ea_import.chain.result', 'log_id', 'Import Results', ),
    }
ea_import_log()


class ea_import_log_line(osv.osv):
    _name = 'ea_import.log.line'
    _columns = {
        'name': fields.char('Log', size=516,),
        'log_id': fields.many2one('ea_import.log', 'Log ID', readonly=True),
    }
ea_import_log_line()
