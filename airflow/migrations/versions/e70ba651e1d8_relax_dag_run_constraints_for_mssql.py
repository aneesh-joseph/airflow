#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""relax dag_run constraints for mssql

Revision ID: e70ba651e1d8
Revises: 5ccc55a461b1
Create Date: 2021-04-06 15:22:02.197726

"""
from collections import defaultdict

from alembic import op

# revision identifiers, used by Alembic.
revision = 'e70ba651e1d8'
down_revision = '5ccc55a461b1'
branch_labels = None
depends_on = None


def drop_constraint(operator, constraint_dict):
    """
    Drop a primary key or unique constraint

    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """


def get_table_constraints(conn, table_name):
    """
    This function return primary and unique constraint
    along with column name. some tables like task_instance
    is missing primary key constraint name and the name is
    auto-generated by sql server. so this function helps to
    retrieve any primary or unique constraint name.

    :param conn: sql connection object
    :param table_name: table name
    :return: a dictionary of ((constraint name, constraint type), column name) of table
    :rtype: defaultdict(list)
    """
    query = """SELECT tc.CONSTRAINT_NAME , tc.CONSTRAINT_TYPE, ccu.COLUMN_NAME
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
     JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
     WHERE tc.TABLE_NAME = '{table_name}' AND
     (tc.CONSTRAINT_TYPE = 'PRIMARY KEY' or UPPER(tc.CONSTRAINT_TYPE) = 'UNIQUE')
    """.format(
        table_name=table_name
    )
    result = conn.execute(query).fetchall()
    constraint_dict = defaultdict(list)
    for constraint, constraint_type, column in result:
        constraint_dict[(constraint, constraint_type)].append(column)
    return constraint_dict


def upgrade():
    """Apply relax dag_run constraints for mssql"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        constraint_dict = get_table_constraints(conn, 'dag_run')
        for constraint, columns in constraint_dict.items():
            if 'dag_id' in columns:
                if constraint[1].lower().startswith("unique"):
                    op.drop_constraint(constraint[0], 'dag_run', type_='unique')
        # create filtered indexes
        conn.execute(
            """CREATE UNIQUE NONCLUSTERED INDEX idx_not_null_dag_id_execution_date
                     ON dag_run(dag_id,execution_date)
                     WHERE dag_id IS NOT NULL and execution_date is not null"""
        )
        conn.execute(
            """CREATE UNIQUE NONCLUSTERED INDEX idx_not_null_dag_id_run_id
                     ON dag_run(dag_id,run_id)
                     WHERE dag_id IS NOT NULL and run_id is not null"""
        )


def downgrade():
    """Unapply relax dag_run constraints for mssql"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.create_unique_constraint('UQ__dag_run__dag_id_run_id', 'dag_run', ['dag_id', 'run_id'])
        op.create_unique_constraint(
            'UQ__dag_run__dag_id_execution_date', 'dag_run', ['dag_id', 'execution_date']
        )
        op.drop_index('idx_not_null_dag_id_execution_date', table_name='dag_run')
        op.drop_index('idx_not_null_dag_id_run_id', table_name='dag_run')