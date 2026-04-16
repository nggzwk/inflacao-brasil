from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, Numeric, PrimaryKeyConstraint, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..database.base import Base


class PriceObservation(Base):
    __tablename__ = "price_observation"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    reference_date: Mapped[date] = mapped_column(Date, nullable=False)
    month_ref: Mapped[date] = mapped_column(Date, nullable=False)

    rede: Mapped[str] = mapped_column(Text, nullable=False)
    endereco: Mapped[str | None] = mapped_column(Text)
    produto: Mapped[str] = mapped_column(Text, nullable=False)
    marca: Mapped[str | None] = mapped_column(Text)

    preco: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    qtd_embalagem: Mapped[str | None] = mapped_column(Text)
    unidade_sigla: Mapped[str | None] = mapped_column(Text)

    categoria_score: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    produto_categoria: Mapped[int | None] = mapped_column(Integer)
    produto_subcategoria: Mapped[int | None] = mapped_column(Integer)

    source_file: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index(
            "ix_price_observation_item_key",
            "qtd_embalagem",
            "unidade_sigla",
            "produto_categoria",
            "produto_subcategoria",
        ),
        Index("ix_price_observation_category_month", "produto_categoria", "produto_subcategoria", "month_ref"),
        Index("ix_price_observation_month_ref", "month_ref"),
    )


class ItemKey(Base):
    __tablename__ = "item_key"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    qtd_embalagem: Mapped[str] = mapped_column(Text, nullable=False)
    unidade_sigla: Mapped[str] = mapped_column(Text, nullable=False)
    produto_categoria: Mapped[int] = mapped_column(Integer, nullable=False)
    produto_subcategoria: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    monthly_prices: Mapped[list[ItemMonthlyPrice]] = relationship(back_populates="item", cascade="all, delete-orphan")
    basket_items: Mapped[list[BasketItem]] = relationship(back_populates="item", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint(
            "qtd_embalagem",
            "unidade_sigla",
            "produto_categoria",
            "produto_subcategoria",
            name="uq_item_key",
        ),
        Index("ix_item_key_category", "produto_categoria", "produto_subcategoria"),
    )


class ItemMonthlyPrice(Base):
    __tablename__ = "item_monthly_price"

    item_id: Mapped[int] = mapped_column(ForeignKey("inflacao_brasil.item_key.id", ondelete="CASCADE"), nullable=False)
    month_ref: Mapped[date] = mapped_column(Date, nullable=False)

    median_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    avg_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    obs_count: Mapped[int] = mapped_column(Integer, nullable=False)
    min_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    max_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    item: Mapped[ItemKey] = relationship(back_populates="monthly_prices")

    __table_args__ = (
        PrimaryKeyConstraint("item_id", "month_ref", name="pk_item_monthly_price"),
        Index("ix_item_monthly_price_month", "month_ref"),
    )


class Basket(Base):
    __tablename__ = "basket"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    items: Mapped[list[BasketItem]] = relationship(back_populates="basket", cascade="all, delete-orphan")


class BasketItem(Base):
    __tablename__ = "basket_item"

    basket_id: Mapped[int] = mapped_column(ForeignKey("inflacao_brasil.basket.id", ondelete="CASCADE"), nullable=False)
    item_id: Mapped[int] = mapped_column(ForeignKey("inflacao_brasil.item_key.id", ondelete="CASCADE"), nullable=False)
    weight: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False, default=Decimal("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    basket: Mapped[Basket] = relationship(back_populates="items")
    item: Mapped[ItemKey] = relationship(back_populates="basket_items")

    __table_args__ = (PrimaryKeyConstraint("basket_id", "item_id", name="pk_basket_item"),)
