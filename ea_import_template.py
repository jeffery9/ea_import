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
from psycopg2.extensions import adapt


class ea_import_template(osv.osv):
    _name = 'ea_import.template'
    _columns = {
        'name': fields.char('Name', size=256),
        'target_model_id': fields.many2one('ir.model', 'Target Model'),
        'test_input_file': fields.binary('Test Importing File'),
        'update': fields.boolean('Update exist',),
        'update_current': fields.boolean('Update only current records',),
        'create_new': fields.boolean('Create New',),
        'create_unique_only': fields.boolean('Create Unique only', help="Create record only if one matching key does not already exist.  Do not use with 'update'."),
        'line_ids': fields.one2many('ea_import.template.line', 'template_id', 'Template Lines', ),
        }

    def generate_record(self, cr, uid, ids, record_list, row_number, context={}):
        result = []
        if len(record_list):
            for template in self.browse(cr, uid, ids, context=context):
                target_model_pool = self.pool.get(template.target_model_id.model)
                record = {}
                upd_key = []
                updated = False
                ready_to_create_new = True
                for template_line in template.line_ids:
                    field_name = template_line.target_field.name
                    value = template_line.get_field(record_list, row_number, testing=True, context=context)
                    if value:
                        record.update({field_name: value})
                        if template_line.key_field:
                            upd_key.append((template_line.target_field.name, '=', value))
                    else:
                        if template_line.required:
                            ready_to_create_new = False
                if template.create_new and template.create_unique_only and upd_key:
                    #check if record matching key already exists
                    if self.low_level_search(cr, uid, ids, upd_key, context=context):
                        log_notes = "Record already exists - skipping ", record
                        context['import_log'].append(log_notes)
                        return result
                    else:
                        new_rec_id = target_model_pool.create(cr, uid, record, context=context)
                        new_rec = target_model_pool.browse(cr, uid, new_rec_id)
                        if new_rec.name:
                            log_row_name = new_rec.name
                        else:
                            log_row_name = ''
                        log_notes = "creating ", target_model_pool._name, "- name = ", new_rec.name, "- data = ", record
                        context['import_log'].append(log_notes)
                        result.append(new_rec_id)
                        return result
                if template.update and upd_key:
                    if template.update_current:
                        upd_key.append(('create_date', '>', context['time_of_start']))
                    updating_record_id = self.low_level_search(cr, uid, ids, upd_key, context=context)
                    if updating_record_id:
                        existing_rec = target_model_pool.browse(cr, uid, updating_record_id)[0]
                        if existing_rec.name:
                            log_row_name = existing_rec.name
                        else:
                            log_row_name = ''
                        if self.need_to_update(cr, uid, target_model_pool, updating_record_id, record, context=context):
                            target_model_pool.write(cr, uid, updating_record_id, record, context=context)
                            result.append(updating_record_id[0])
                            log_notes = "update ", target_model_pool._name, "- record id = ", updating_record_id, "- name = ", log_row_name, "- data = ", record
                            context['import_log'].append(log_notes)
                        else:
                            log_notes = "no update ", target_model_pool._name, "- record id = ", updating_record_id, "- name = ", log_row_name, "- NO CHANGES REQUIRED - data = ", record
                            context['import_log'].append(log_notes)
                        updated = True
                if not updated and template.create_new and ready_to_create_new:
                    new_rec_id = target_model_pool.create(cr, uid, record, context=context)
                    new_rec = target_model_pool.browse(cr, uid, new_rec_id)
                    if new_rec.name:
                        log_row_name = new_rec.name
                    else:
                        log_row_name = ''
                    log_notes = "creating ", target_model_pool._name, "- name = ", new_rec.name, "- data = ", record
                    context['import_log'].append(log_notes)
                    result.append(new_rec_id)
        return result

    def get_related_id(self, cr, uid, ids, input_list, row_number, context={}):
        result = []
        for template in self.browse(cr, uid, ids, context=context):
            key = []
            for template_line in template.line_ids:
                if template_line.key_field:
                    field_name = template_line.target_field.name
                    value = template_line.get_field(input_list, row_number, testing=True)
                    if value:
                        key.append((field_name, '=', value))
            if template.update_current:
                key.append(('create_date', '>', context['time_of_start']))
            result = self.low_level_search(cr, uid, [template.id], key, context=context)
            return result

    def low_level_search(self, cr, uid, ids, key_list, context={}):
        for template in self.browse(cr, uid, ids, context=context):
            target_model_pool = self.pool.get(template.target_model_id.model)
            where_string = "WHERE id IS NOT NULL\n"
            for key_sub_list in key_list:
                second_parametr = adapt(key_sub_list[2]).getquoted()
                where_string += "AND {0} {1} {2} \n".format(key_sub_list[0], key_sub_list[1], second_parametr)
            cr.execute("""
                        SELECT *
                        FROM %s
                        %s""" % (target_model_pool._table, where_string))
            result = cr.fetchall()
            result = [id_numders[0] for id_numders in result]
            return result

    def need_to_update(self, cr, uid, target_model_pool, updating_record_id, record, context={}):
        for old_record in target_model_pool.read(cr, uid, updating_record_id, context=context):
            filtered_old_record = {}
            for key, value in old_record.items():
                if isinstance(value, tuple):
                    filtered_old_record[key] = value[0]
                elif isinstance(value, dict):
                    continue
                else:
                    filtered_old_record[key] = value
        return any([filtered_old_record.get(key) != value for key, value in record.items()])

ea_import_template()
# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
