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

"""change ts columns to datetime on mssql

Revision ID: 83f031fd9f1c
Revises: a66efa278eea
Create Date: 2020-07-23 12:22:02.197726

"""

# revision identifiers, used by Alembic.
revision = '83f031fd9f1c'
down_revision = 'a66efa278eea'
branch_labels = None
depends_on = None

from alembic import op
from collections import defaultdict
import sqlalchemy as sa
from sqlalchemy.dialects import mssql


def use_date_time(conn):
    result = conn.execute(
        """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion""").fetchone()
    mssql_version = result[0]
    return mssql_version in ("2000", "2005")

def upgrade():
    """
    Change timestamp to datetime2/datetime when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        if use_date_time(conn):
            op.alter_column(
                table_name="dag_code",
                column_name="last_updated",
                type_=mssql.DATETIME,
                nullable=False,
            )
            op.alter_column(
                table_name="rendered_task_instance_fields",
                column_name="execution_date",
                type_=mssql.DATETIME,
                nullable=False,
            )
        else:
            op.alter_column(
                table_name="dag_code",
                column_name="last_updated",
                type_=mssql.DATETIME2(precision=6),
                nullable=False,
            )
            op.alter_column(
                table_name="rendered_task_instance_fields",
                column_name="execution_date",
                type_=mssql.DATETIME2(precision=6),
                nullable=False,
            )


def downgrade():
    """
    Change datetime2/datetime back to timestamp when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.alter_column(
            table_name="dag_code",
            column_name="last_updated",
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False,
        )
        op.alter_column(
            table_name="rendered_task_instance_fields",
            column_name="execution_date",
            type_=sa.TIMESTAMP(timezone=True),
            nullable=False,
        )
