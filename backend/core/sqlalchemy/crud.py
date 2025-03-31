from typing import Optional

from fastapi import HTTPException, Response
from sqlalchemy import asc, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.sqlalchemy.orm import Orm


class Crud:
    def __init__(self, model):
        self.model = model

    def get_not_found_text(self, obj_id: int):
        return f"{self.model.__name__} with ID №{obj_id} not found"

    @staticmethod
    def get_unique_fields(model):
        return [column.name for column in model.__table__.columns if column.unique]

    @staticmethod
    async def check_field_unique(
        model, data: dict, field: str, session: AsyncSession, obj_id=None
    ):
        async with session.begin():
            field_value = data.get(field)
            filter_data = {field: field_value}
            exclude_data = {"id": obj_id} if obj_id else None

            query = await Orm.filter_by(model, filter_data, session, exclude_data)

            if query.scalar() is not None:
                raise HTTPException(
                    400, f"{model.__name__} with {field}={field_value} already exists"
                )

    @classmethod
    async def check_unique_fields(
        cls, model, data: dict, session: AsyncSession, obj_id=None
    ):
        unique_fields = cls.get_unique_fields(model)

        for field in unique_fields:
            await cls.check_field_unique(model, data, field, session, obj_id)

    async def create(self, data, session: AsyncSession, relations=None):
        """
        Method that creates a new instance of the model

        :param:
        - `data`: Dictionary with data to create a new record.
        - `session`: The current database session.

        :return:
            `Created object.`
        """
        model_dump = data if isinstance(data, dict) else data.model_dump()

        await self.check_unique_fields(self.model, model_dump, session)

        instance = await Orm.create(self.model, model_dump, session)

        if relations:
            instance = await Orm.scalar(
                self.model, session, self.model.id == instance.id, relations
            )

        return instance

    async def create_bulk(
        self, data, bulk_key: str, session: AsyncSession, return_data=None
    ):
        data_list = data.model_dump()[bulk_key]

        result = await Orm.insert(self.model, data_list, session, return_data)
        return {bulk_key: [{"id": row[0]} for row in result.fetchall()]}

    async def delete(
        self,
        obj_id: int,
        session: AsyncSession,
        status: int = 204,
        content: dict = None,
    ):
        """
        Method that deletes the instance of the model

        :param:
        - `obj_id`: ID of the instance to delete.
        - `session`: The current database session.

        :return:
            `Response(204).`
        """
        book = await Orm.scalar(self.model, session, self.model.id == obj_id)

        if not book:
            raise HTTPException(404, self.get_not_found_text(obj_id))

        await session.delete(book)
        await session.commit()

        return Response(content=content, status_code=status)

    async def list(
        self,
        session: AsyncSession,
        relations=None,
        sort_field: Optional[str] = None,
        sort_order: Optional[str] = "asc",
        filters: list = None,
        **sql_methods
    ):
        """
        Метод для получения списка объектов с фильтрацией и сортировкой.

        :param session: Текущая сессия базы данных.
        :param relations: Связанные поля.
        :param sort_field: Поле для сортировки.
        :param sort_order: Порядок сортировки ('asc' или 'desc').
        :param filters: Произвольные параметры для фильтрации.

        :return: Список объектов с примененными фильтрацией и сортировкой.
        """

        query = select(self.model)

        if filters:
            query = query.filter(*filters)

        if sort_field or sort_order:
            sort_field = sort_field or "id"
            sort_column = getattr(self.model, sort_field, None)

            if sort_column:
                order = (
                    asc(sort_column)
                    if sort_order.lower() == "asc"
                    else desc(sort_column)
                )
                query = query.order_by(order)

        if relations:
            query = Orm.get_query_with_relations(query, relations)

        for method, value in sql_methods.items():
            query = getattr(query, method)(value)

        execution = await session.execute(query)
        return execution.scalars().all()

    async def retrieve(self, obj_id: int, session: AsyncSession, relations=None):
        """
        Method that retrieves an instance of the model by ID

        :param:
        - `obj_id`: ID of the instance to retrieve.
        - `session`: The current database session.

        :return:
            `Object instance.`
        """
        obj = await Orm.scalar(self.model, session, self.model.id == obj_id, relations)
        if not obj:
            raise HTTPException(404, self.get_not_found_text(obj_id))

        return obj

    async def update(
        self, data: dict, obj_id: int, session: AsyncSession, relations=None
    ):
        """
        Method that updates an instance of the model

        :param:
        - `data`: Dictionary with updated data.
        - `obj_id`: ID of the instance to update.
        - `session`: The current database session.

        :return:
            `Updated object.`
        """
        await self.check_unique_fields(self.model, data, session, obj_id)

        obj = await Orm.scalar(self.model, session, self.model.id == obj_id, relations)

        if not obj:
            raise HTTPException(404, self.get_not_found_text(obj_id))

        await Orm.update(obj, data, session)
        await session.refresh(obj)

        return obj
