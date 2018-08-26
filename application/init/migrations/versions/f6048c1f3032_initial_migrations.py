"""Initial migrations

Revision ID: f6048c1f3032
Revises: 
Create Date: 2018-08-26 17:16:00.036000

"""
import geoalchemy2
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'f6048c1f3032'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('cases',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('report_date', sa.Date(), nullable=True),
                    sa.Column('location', geoalchemy2.types.Geometry(geometry_type='POINT', srid=3857), nullable=True),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_table('distribution_margins',
                    sa.Column('number_of_cases', sa.Integer(), nullable=False),
                    sa.Column('close_in_space_and_time', sa.Integer(), nullable=False),
                    sa.Column('probability', sa.Float(), nullable=True),
                    sa.Column('cumulative_probability', sa.Float(), nullable=True),
                    sa.Column('close_space', sa.Integer(), nullable=False),
                    sa.Column('close_time', sa.Integer(), nullable=False),
                    sa.PrimaryKeyConstraint('number_of_cases', 'close_in_space_and_time', 'close_space', 'close_time')
                    )
    op.create_index(op.f('ix_distribution_margins_close_in_space_and_time'), 'distribution_margins',
                    ['close_in_space_and_time'], unique=False)
    op.create_index(op.f('ix_distribution_margins_close_space'), 'distribution_margins', ['close_space'], unique=False)
    op.create_index(op.f('ix_distribution_margins_close_time'), 'distribution_margins', ['close_time'], unique=False)
    op.create_index(op.f('ix_distribution_margins_number_of_cases'), 'distribution_margins', ['number_of_cases'],
                    unique=False)
    op.create_table('risk',
                    sa.Column('risk_date', sa.Date(), nullable=False),
                    sa.Column('lat', sa.Float(), nullable=False),
                    sa.Column('long', sa.Float(), nullable=False),
                    sa.Column('number_of_cases', sa.Integer(), nullable=True),
                    sa.Column('close_pairs', sa.Integer(), nullable=True),
                    sa.Column('close_space', sa.Integer(), nullable=True),
                    sa.Column('close_time', sa.Integer(), nullable=True),
                    sa.Column('cumulative_probability', sa.Float(), nullable=True),
                    sa.PrimaryKeyConstraint('risk_date', 'lat', 'long')
                    )


def downgrade():

    op.drop_table('spatial_ref_sys')
    op.drop_table('risk')
    op.drop_index(op.f('ix_distribution_margins_number_of_cases'), table_name='distribution_margins')
    op.drop_index(op.f('ix_distribution_margins_close_time'), table_name='distribution_margins')
    op.drop_index(op.f('ix_distribution_margins_close_space'), table_name='distribution_margins')
    op.drop_index(op.f('ix_distribution_margins_close_in_space_and_time'), table_name='distribution_margins')
    op.drop_table('distribution_margins')
    op.drop_table('cases')
