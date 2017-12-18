#!/usr/bin/env python
import os
from app import create_app, db
from app.models import User, Follow, Role, Permission, Post
from flask_script import Manager, Shell
from flask_migrate import Migrate, MigrateCommand

application = create_app(os.getenv('FLASK_CONFIG') or 'default')
manager = Manager(application)
migrate = Migrate(application, db)


def make_shell_context():
    return dict(app=application, db=db, User=User, Follow=Follow, Role=Role,
                Permission=Permission, Post=Post)
manager.add_command("shell", Shell(make_context=make_shell_context))
manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
    # manager.run()
    application.run()
