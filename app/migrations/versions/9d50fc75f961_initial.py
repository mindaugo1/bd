"""initial

Revision ID: 9d50fc75f961
Revises: 
Create Date: 2022-03-13 15:56:27.576102

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9d50fc75f961'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('customer',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('customer_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('customer_id')
    )
    op.create_index(op.f('ix_customer_id'), 'customer', ['id'], unique=False)
    op.create_table('plan',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('rate_plan_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('rate_plan_id')
    )
    op.create_index(op.f('ix_plan_id'), 'plan', ['id'], unique=False)
    op.create_table('service',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('service_type', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('service_type')
    )
    op.create_index(op.f('ix_service_id'), 'service', ['id'], unique=False)
    op.create_table('event',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('customer_fk', sa.Integer(), nullable=True),
    sa.Column('start_date', sa.DateTime(), nullable=True),
    sa.Column('service_type_fk', sa.Integer(), nullable=True),
    sa.Column('rate_plan_fk', sa.Integer(), nullable=True),
    sa.Column('billing_flag_1', sa.Integer(), nullable=True),
    sa.Column('billing_flag_2', sa.Integer(), nullable=True),
    sa.Column('duration', sa.Integer(), nullable=True),
    sa.Column('charge', sa.Numeric(scale=6), nullable=True),
    sa.Column('month', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['customer_fk'], ['customer.id'], ),
    sa.ForeignKeyConstraint(['rate_plan_fk'], ['plan.id'], ),
    sa.ForeignKeyConstraint(['service_type_fk'], ['service.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_event_created_at'), 'event', ['created_at'], unique=False)
    op.create_index(op.f('ix_event_customer_fk'), 'event', ['customer_fk'], unique=False)
    op.create_index(op.f('ix_event_id'), 'event', ['id'], unique=False)
    op.create_index(op.f('ix_event_rate_plan_fk'), 'event', ['rate_plan_fk'], unique=False)
    op.create_index(op.f('ix_event_service_type_fk'), 'event', ['service_type_fk'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_event_service_type_fk'), table_name='event')
    op.drop_index(op.f('ix_event_rate_plan_fk'), table_name='event')
    op.drop_index(op.f('ix_event_id'), table_name='event')
    op.drop_index(op.f('ix_event_customer_fk'), table_name='event')
    op.drop_index(op.f('ix_event_created_at'), table_name='event')
    op.drop_table('event')
    op.drop_index(op.f('ix_service_id'), table_name='service')
    op.drop_table('service')
    op.drop_index(op.f('ix_plan_id'), table_name='plan')
    op.drop_table('plan')
    op.drop_index(op.f('ix_customer_id'), table_name='customer')
    op.drop_table('customer')
    # ### end Alembic commands ###
