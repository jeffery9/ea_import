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
try:
    from openerp.addons.ea_import.ir_actions_report import CsvExportOpenERPInterface
except:  # Used to be compatible with v6.0, you need to put this module directly into your server/bin/addons path
    from addons.ea_import.ir_actions_report import CsvExportOpenERPInterface


class ea_export_config(osv.osv):
    _name = 'ea_export.config'
    _columns = {
        'name': fields.char('Name', size=256, required=True),
        'filename': fields.char('Export filename', size=256, required=True),
        'query': fields.text('SQL Query', help="SQL/Object Query For sql use normal syntax: select ref, name from res_partner where id in (%s), etc."),
        'query_type': fields.selection([
            ("sql", 'SQL'),
            ("object", 'Object'),
        ], 'Query Type', required=True),
        'header': fields.boolean('Header'),
        'delimiter': fields.selection([
            (",", '<,> - Coma'),
            (";", '<;> - Semicolon'),
            ("\t", '<TAB> - Tab'),
            (" ", '<SPACE> - Space'),
        ], 'Delimiter', required=True),
        'quotechar': fields.selection([
            ("'", '<\'> - Single quotation mark'),
            ('"', '<"> - Double quotation mark'),
            ('None', 'None'),
        ], 'Quotechar', ),
        'charset': fields.selection([
            ('us-ascii', 'US-ASCII'),
            ('utf-7', 'Unicode (UTF-7)'),
            ('utf-8', 'Unicode (UTF-8)'),
            ('utf-16', 'Unicode (UTF-16)'),
            ('windows-1250', 'Central European (Windows 1252)'),
            ('windows-1251', 'Cyrillic (Windows 1251)'),
            ('iso-8859-1', 'Western European (ISO)'),
            ('iso-8859-15', 'Latin 9 (ISO)'),
        ], 'Encoding', required=True),
        'model_id': fields.many2one('ir.model', 'Related document model', required=True,),
        'ir_act_report_id': fields.many2one('ir.actions.report.xml', 'Report action', readonly=True, ),
        'ir_value_id': fields.many2one('ir.values', 'Sidebar button', readonly=True, ),
        }

    _defaults = {
        'query_type': 'sql',
        'delimiter': ",",
        'charset': 'utf-8',
        'delimiter': None,
    }

    def create_action(self, cr, uid, ids, context=None):
        vals = {}
        action_obj = self.pool.get('ir.actions.report.xml')
        for chain in self.browse(cr, uid, ids, context=context):
            model_name = chain.model_id.model
            button_name = 'Export to CSV (%s)' % chain.name
            vals['ir_act_report_id'] = action_obj.create(cr, uid, {
                'name': button_name,
                'type': 'ir.actions.report.xml',
                'model': model_name,
                'view_type': 'form',
                'res_id': chain.id,
                'target': 'new',
                'auto_refresh': True,
                'csv_export': True,
                'report_type': 'csv',
                'report_name': chain.filename + '.csv.export',
                'export_config_id': chain.id,
            }, context)
            vals['ir_value_id'] = self.pool.get('ir.values').create(cr, uid, {
                 'name': button_name,
                 'model': model_name,
                 'key2': 'client_print_multi',
                 'value': "ir.actions.report.xml," + str(vals['ir_act_report_id']),
                 'object': True,
             }, context)
        self.write(cr, uid, ids, {
                    'ir_act_report_id': vals.get('ir_act_report_id', False),
                    'ir_value_id': vals.get('ir_value_id', False),
                }, context)
        return True

    def unlink_action(self, cr, uid, ids, context=None):
        for chain in self.browse(cr, uid, ids, context=context):
            if chain.ir_act_report_id:
                self.pool.get('ir.actions.report.xml').unlink(cr, uid, [chain.ir_act_report_id.id], context)
            if chain.ir_value_id:
                ir_values_obj = self.pool.get('ir.values')
                ir_values_obj.unlink(cr, uid, chain.ir_value_id.id, context)
        return True

    def generate_csv(self, cr, uid, ids, context={}):
        for config in self.browse(cr, uid, ids, context=context):
            if not config.ir_act_report_id:
                raise osv.except_osv(('Error !'), ("Create report first"))

            name = 'report.' + config.ir_act_report_id.report_name
            config_value = {
                            'report_name': config.ir_act_report_id.report_name,
                            'config_id': config.id,
                            'config_query': config.query,
                            'delimiter': config.delimiter,
                            'quotechar': config.quotechar,
                            'header': config.header,
                            'query_type': config.query_type
            }
            export_class = CsvExportOpenERPInterface(name, config_value, context=context)
        output, format = export_class.create(cr, uid, ids, {'report_type': u'csv'}, {'active_ids': [], 'active_model': 'ea_export.config'})
        if output:
            return {
                    'type': 'ir.actions.report.xml',
                    'report_name': config.ir_act_report_id.report_name,
            }

ea_export_config()

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
