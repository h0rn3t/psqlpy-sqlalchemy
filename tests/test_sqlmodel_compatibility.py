#!/usr/bin/env python3
"""
Tests for SQLModel compatibility with psqlpy-sqlalchemy dialect
"""

import unittest
from typing import Optional

from sqlalchemy import create_engine
from sqlmodel import Field, Session, SQLModel, select


class Hero(SQLModel, table=True):
    """Test model for SQLModel compatibility tests"""

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: Optional[int] = None


class TestSQLModelCompatibility(unittest.TestCase):
    """Test cases for SQLModel compatibility with psqlpy-sqlalchemy dialect"""

    def setUp(self):
        """Set up test fixtures before each test method."""

        self.engine = create_engine("sqlite:///:memory:")

        # Create all tables
        SQLModel.metadata.create_all(self.engine)

        # Add some test data
        with Session(self.engine) as session:
            session.add(
                Hero(name="Deadpond", secret_name="Dive Wilson", age=30)
            )
            session.add(
                Hero(name="Spider-Boy", secret_name="Pedro Parqueador", age=16)
            )
            session.add(
                Hero(name="Rusty-Man", secret_name="Tommy Sharp", age=48)
            )
            session.commit()

    def tearDown(self):
        """Clean up after each test method."""
        SQLModel.metadata.drop_all(self.engine)

    def test_sqlmodel_create_and_read(self):
        """Test creating and reading SQLModel objects"""
        # Create a new hero
        with Session(self.engine) as session:
            hero = Hero(
                name="Captain America", secret_name="Steve Rogers", age=100
            )
            session.add(hero)
            session.commit()
            session.refresh(hero)

            # Verify the hero was created with an ID
            self.assertIsNotNone(hero.id)

            # Read the hero back
            statement = select(Hero).where(Hero.name == "Captain America")
            result = session.exec(statement).first()

            # Verify the hero was read correctly
            self.assertIsNotNone(result)
            self.assertEqual(result.name, "Captain America")
            self.assertEqual(result.secret_name, "Steve Rogers")
            self.assertEqual(result.age, 100)

    def test_sqlmodel_update(self):
        """Test updating SQLModel objects"""
        with Session(self.engine) as session:
            # Find a hero to update
            statement = select(Hero).where(Hero.name == "Spider-Boy")
            hero = session.exec(statement).first()
            self.assertIsNotNone(hero)

            # Update the hero
            hero.age = 17  # Birthday!
            session.add(hero)
            session.commit()

            # Verify the update
            updated_hero = session.exec(statement).first()
            self.assertEqual(updated_hero.age, 17)

    def test_sqlmodel_delete(self):
        """Test deleting SQLModel objects"""

        with Session(self.engine) as session:
            # Count heroes before deletion
            statement = select(Hero)
            heroes_before = session.exec(statement).all()
            count_before = len(heroes_before)

            # Find and delete a hero
            statement = select(Hero).where(Hero.name == "Rusty-Man")
            hero = session.exec(statement).first()
            self.assertIsNotNone(hero)

            session.delete(hero)
            session.commit()

            # Verify the deletion
            statement = select(Hero)
            heroes_after = session.exec(statement).all()
            count_after = len(heroes_after)

            self.assertEqual(count_after, count_before - 1)

            statement = select(Hero).where(Hero.name == "Rusty-Man")
            deleted_hero = session.exec(statement).first()
            self.assertIsNone(deleted_hero)

    def test_sqlmodel_relationships(self):
        """Test SQLModel relationship handling"""

        class Team(SQLModel, table=True):
            id: Optional[int] = Field(default=None, primary_key=True)
            name: str
            headquarters: str

        class HeroWithTeam(SQLModel, table=True):
            id: Optional[int] = Field(default=None, primary_key=True)
            name: str
            secret_name: str
            age: Optional[int] = None
            team_id: Optional[int] = Field(default=None, foreign_key="team.id")

        # Create tables for these models
        Team.metadata.create_all(self.engine)
        HeroWithTeam.metadata.create_all(self.engine)

        # Test data with relationships
        with Session(self.engine) as session:
            # Create teams
            avengers = Team(name="Avengers", headquarters="New York")
            justice_league = Team(
                name="Justice League", headquarters="Washington"
            )
            session.add(avengers)
            session.add(justice_league)
            session.commit()

            # Create heroes with team relationships
            hero1 = HeroWithTeam(
                name="Iron Man",
                secret_name="Tony Stark",
                age=45,
                team_id=avengers.id,
            )
            hero2 = HeroWithTeam(
                name="Batman",
                secret_name="Bruce Wayne",
                age=40,
                team_id=justice_league.id,
            )
            session.add(hero1)
            session.add(hero2)
            session.commit()

            # Query heroes with their teams
            statement = select(HeroWithTeam).where(
                HeroWithTeam.team_id == avengers.id
            )
            avengers_heroes = session.exec(statement).all()

            self.assertEqual(len(avengers_heroes), 1)
            self.assertEqual(avengers_heroes[0].name, "Iron Man")

            # Clean up
            HeroWithTeam.metadata.drop_all(self.engine)
            Team.metadata.drop_all(self.engine)


class TestSQLiteDialectWithSQLModel(unittest.TestCase):
    """Test cases for SQLite dialect with SQLModel"""

    def test_sqlite_dialect_with_sqlmodel(self):
        """Test using SQLite dialect with SQLModel"""
        # This test uses SQLite in-memory database for testing

        # Create engine with SQLite dialect
        engine = create_engine("sqlite:///:memory:")

        # Create all tables
        SQLModel.metadata.create_all(engine)

        # Test basic operations
        with Session(engine) as session:
            # Create
            hero = Hero(
                name="Black Widow", secret_name="Natasha Romanoff", age=35
            )
            session.add(hero)
            session.commit()
            session.refresh(hero)

            # Read
            statement = select(Hero).where(Hero.name == "Black Widow")
            result = session.exec(statement).first()

            # Verify
            assert result is not None
            assert result.name == "Black Widow"
            assert result.secret_name == "Natasha Romanoff"
            assert result.age == 35

            # Clean up
            SQLModel.metadata.drop_all(engine)


if __name__ == "__main__":
    unittest.main()
