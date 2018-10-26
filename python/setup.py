from setuptools import setup

setup(name='sparktpcds',
      version='0.1.4',
      description='Base library to access the Spark Data Manager.',
      url='https://github.com/aaronvh/spark-tpcds',
      author='Aaron Van Hecken',
      author_email='aaron.van.hecken@gmail.com',
      packages=['sparktpcds'],
      install_requires=['pyspark'],
      zip_safe=False)
