""" This is a test of the chain
    TransformationClient -> TransformationManagerHandler -> TransformationDB

    It supposes that the DB is present, and that the service is running
    The test_inputDataQueries also requires that a Catalog service is running and that it configured with 'MetaCatalog = True' in the CS, e.g.:

    Resources
    {
     FileCatalogs
     {
      DIRACFileCatalog
       {
        MetaCatalog = True
        ...
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, os, json

from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

class TestClientTransformationTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = TransformationClient()

  def tearDown( self ):
    pass


class TransformationClientChain( TestClientTransformationTestCase ):

  def test_addAndRemove( self ):
    # add
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    self.assert_( res['OK'] )
    transID = res['Value']

    # try to add again (this should fail)
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    self.assertFalse( res['OK'] )

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationParameters( transID, 'Status' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'TransformationCleaned' )

    # really delete
    res = self.transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # delete non existing one (fails)
    res = self.transClient.deleteTransformation( transID )
    self.assertFalse( res['OK'] )


  def test_addTasksAndFiles( self ):
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transID = res['Value']

    # add tasks - no lfns
    res = self.transClient.addTaskForTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 1 )
    res = self.transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )

    # add tasks - with lfns
    res = self.transClient.addTaskForTransformation( transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt'] )
    # fails because the files are not present
    self.assertFalse( res['OK'] )
    # so now adding them
    res = self.transClient.addFilesToTransformation( transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt',
                                                               '/aa/lfn.3.txt', '/aa/lfn.4.txt'] )
    self.assert_( res['OK'] )

    # now it should be ok
    res = self.transClient.addTaskForTransformation( transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt'] )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transID, ['/aa/lfn.3.txt', '/aa/lfn.4.txt'] )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 3 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1
    res = self.transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 4 )
    for f in res['Value']:
      self.assertEqual( f['Status'], 'Assigned' )

    # now adding a new Transformation with new tasks, and introducing a mix of insertion,
    # to test that the trigger works as it should
    res = self.transClient.addTransformation( 'transName-new', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transIDNew = res['Value']
    # add tasks - no lfns
    res = self.transClient.addTaskForTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 2 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1
    # now mixing things
    res = self.transClient.addTaskForTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 5 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1
    res = self.transClient.getTransformationTasks( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 3 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )

    res = self.transClient.cleanTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )
    res = self.transClient.getTransformationTasks( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )

    # delete it in the end
    self.transClient.deleteTransformation( transID )
    self.transClient.deleteTransformation( transIDNew )

  def test_mix( self ):
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transID = res['Value']

    # parameters
    res = self.transClient.setTransformationParameter( transID, 'aParamName', 'aParamValue' )
    self.assert_( res['OK'] )
    res1 = self.transClient.getTransformationParameters( transID, 'aParamName' )
    self.assert_( res1['OK'] )
    res2 = self.transClient.getTransformationParameters( transID, ( 'aParamName', ) )
    self.assert_( res2['OK'] )
    res3 = self.transClient.getTransformationParameters( transID, ['aParamName'] )
    self.assert_( res3['OK'] )
    self.assert_( res1['Value'] == res2['Value'] == res3['Value'] )

    # file status
    lfns = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt', '/aa/lfn.4.txt']
    res = self.transClient.addFilesToTransformation( transID, lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    self.assert_( res['OK'] )
    for f in res['Value']:
      self.assertEqual( f['Status'], 'Unused' )
    res = self.transClient.setFileStatusForTransformation( transID, 'Assigned', lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    for f in res['Value']:
      self.assertEqual( f['Status'], 'Assigned' )
    res = self.transClient.getTransformationStats( transID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'Assigned': 4L, 'Total': 4L} )
    res = self.transClient.setFileStatusForTransformation( transID, 'Unused', lfns )
    # tasks
    res = self.transClient.addTaskForTransformation( transID, lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    taskIDs = []
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      taskIDs.append( task['TaskID'] )
    self.transClient.setTaskStatus( transID, taskIDs, 'Running' )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Running' )
    res = self.transClient.extendTransformation( transID, 5 )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assertEqual( len( res['Value'] ), 6 )
    res = self.transClient.getTasksToSubmit( transID, 5 )
    self.assert_( res['OK'] )

    # logging
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Active' )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationLogging( transID )
    self.assert_( res['OK'] )
    self.assertAlmostEqual( len( res['Value'] ), 4 )

    # delete it in the end
    self.transClient.cleanTransformation( transID )
    self.transClient.deleteTransformation( transID )

  def test_inputDataQuerieswithReplicas( self ):
    # ## Check that there is one MetaCatalog defined
    fc = FileCatalog()
    res = fc.metaCatalogs
    self.assertEqual( len( res ), 1 )
    metaCatalog = res[0]

    # ## Add metadata fields to the DFC
    MDFieldDict = {'particle':'VARCHAR(128)', 'zenith':'int'}
    for MDField in MDFieldDict.keys():
      MDFieldType = MDFieldDict[MDField]
      res = fc.addMetadataField( MDField, MDFieldType )
      self.assert_( res['OK'] )

    # ## Create a directory in the DFC and set the directory metadata
    dirpath1 = '/dir1'
    res = fc.createDirectory( dirpath1 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][dirpath1]['TSCatalog'], True )
    self.assertEqual( res['Value']['Successful'][dirpath1][metaCatalog], True )

    MDdict1 = {'particle':'gamma_diffuse', 'zenith':20}
    res = fc.setMetadata( dirpath1 , MDdict1 )
    self.assert_( res['OK'] )

    #### Add a first file to the DFC and TS Catalog
    filename = 'file1'
    lfn1 = os.path.join( dirpath1, filename )
    fileTuple = ( lfn1, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001' )

    dm = DataManager()
    res = dm.registerFile( fileTuple )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn1][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn1]['TSCatalog'], False )

# ## Create a transformation having a query with replica location requirement that matches the file location
    transClient = TransformationClient()
    MDdict1b = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':'ALPHA-Disk'}
    mqJson1b = json.dumps( MDdict1b )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson1b )
    self.assert_( res['OK'] )
    transID = res['Value']

    # ## Verify that the created file is added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0]['LFN'], lfn1 )

    # # Add a replica of the file with location not matching the transformation query
    replicaDict = {lfn1: {'SE': 'BETA-Disk', 'PFN': 'dips://beta:9148/DataManagement/StorageElement/dir1/file1'}}
    res = fc.addReplica( replicaDict )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn1][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn1]['TSCatalog'], False )  # since there are no matching transformations

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # ## Create another transformation having a query with replica location not matching none of the replicas added to the DFC
    MDdict3 = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':'GAMMA-Disk'}
    mqJson3 = json.dumps( MDdict3 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson3 )
    self.assert_( res['OK'] )
    transID = res['Value']

# ## Verify that no files have been added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assertEqual( len( res['Value'] ) , 0 )

    # # Add a replica of the file to a location matching the transformation query
    replicaDict = {lfn1: {'SE': 'GAMMA-Disk', 'PFN': 'dips://gamma:9148/DataManagement/StorageElement/dir1/file1'}}
    res = fc.addReplica( replicaDict )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn1][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn1]['TSCatalog'], True )  # since there is a matching transformation

    # ## Verify that the file is added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0]['LFN'], lfn1 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )
############################

######## Verify the other operators: not equal, in, not in

    # ## Create another transformation having a query with replica using not equal operator and matching the condition
    MDdict4 = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':{'!=':'ALPHA-Disk'}}
    mqJson4 = json.dumps( MDdict4 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson4 )
    self.assert_( res['OK'] )
    transID = res['Value']

    # ## Verify that the file is added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0]['LFN'], lfn1 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # ## Create another transformation having a query with replica using in operator and matching the condition
    MDdict5 = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':{'in': ['ALPHA-Disk', 'BETA-Disk']}}
    mqJson5 = json.dumps( MDdict5 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson5 )
    self.assert_( res['OK'] )
    transID = res['Value']

    # ## Verify that the file is added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0]['LFN'], lfn1 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # ## Create another transformation having a query with replica using in operator and not matching the condition
    MDdict6 = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':{'in': ['DELTA-Disk', 'EPSILON-Disk']}}
    mqJson6 = json.dumps( MDdict6 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson6 )
    self.assert_( res['OK'] )
    transID = res['Value']

# ## Verify that no files have been added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assertEqual( len( res['Value'] ) , 0 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # ## Create another transformation having a query with replica using not in operator and matching the condition
    MDdict7 = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':{'nin': ['DELTA-Disk', 'EPSILON-Disk']}}
    mqJson7 = json.dumps( MDdict7 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson7 )
    self.assert_( res['OK'] )
    transID = res['Value']

    # ## Verify that the file is added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0]['LFN'], lfn1 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # ## Create another transformation having a query with replica using not in operator and not matching the condition
    MDdict8 = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}, 'SE':{'nin': ['ALPHA-Disk', 'BETA-Disk', 'GAMMA-Disk']}}
    mqJson8 = json.dumps( MDdict8 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson8 )
    self.assert_( res['OK'] )
    transID = res['Value']

    # ## Verify that no files have been added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assertEqual( len( res['Value'] ) , 0 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

# # Remove files from both Catalogs
    res = fc.removeFile( lfn1 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn1][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn1]['TSCatalog'], True )

# # Remove directories from  both Catalogs
    dirlist = [dirpath1]
    res = fc.removeDirectory( dirlist )
    self.assert_( res['OK'] )

# # Remove metadata fields from DFC
    for MDField in MDFieldDict.keys():
      MDFieldType = MDFieldDict[MDField]
      res = fc.deleteMetadataField( MDField )
      self.assert_( res['OK'] )


  def test_inputDataQueries( self ):

    # ## Check that there is one MetaCatalog defined
    fc = FileCatalog()
    res = fc.metaCatalogs
    self.assertEqual( len( res ), 1 )
    metaCatalog = res[0]

    # ## Add metadata fields to the DFC
    MDFieldDict = {'particle':'VARCHAR(128)', 'zenith':'int'}
    for MDField in MDFieldDict.keys():
      MDFieldType = MDFieldDict[MDField]
      res = fc.addMetadataField( MDField, MDFieldType )
      self.assert_( res['OK'] )

    # ## Create a directory in the DFC and set the directory metadata
    dirpath1 = '/dir1'
    res = fc.createDirectory( dirpath1 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][dirpath1]['TSCatalog'], True )
    self.assertEqual( res['Value']['Successful'][dirpath1][metaCatalog], True )

    MDdict1 = {'particle':'gamma_diffuse', 'zenith':20}
    res = fc.setMetadata( dirpath1 , MDdict1 )
    self.assert_( res['OK'] )

#### Add a first file to the DFC and TS Catalog
    filename = 'file1'
    lfn1 = os.path.join( dirpath1, filename )
    fileTuple = ( lfn1, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001' )

    dm = DataManager()
    res = dm.registerFile( fileTuple )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn1][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn1]['TSCatalog'], False )

# ## Create a transformation having a query that matches the file metadata
    transClient = TransformationClient()
    MDdict1b = {'particle':'gamma_diffuse', 'zenith':{"<=": 20}}
    mqJson1b = json.dumps( MDdict1b )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson1b )
    print res
    self.assert_( res['OK'] )
    transID = res['Value']

    # ## Verify that the created file is added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0]['LFN'], lfn1 )

    # # Add a second file to the DFC and TS Catalog having the same metadata
    filename = 'file2'
    lfn2 = os.path.join( dirpath1, filename )
    fileTuple = ( lfn2, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001' )
    res = dm.registerFile( fileTuple )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn2][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn2]['TSCatalog'], True )

    # ## Verify that the second file has been automatically added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][1]['LFN'], lfn2 )

    #### Add a third file to the DFC and TS Catalog having a different metadata set not matching the transformation query
    dirpath2 = '/dir2'
    res = fc.createDirectory( dirpath2 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][dirpath2]['TSCatalog'], True )
    self.assertEqual( res['Value']['Successful'][dirpath2][metaCatalog], True )

    MDdict2 = {'particle':'gamma_diffuse', 'zenith':40}
    res = fc.setMetadata( dirpath2 , MDdict2 )
    self.assert_( res['OK'] )

    fileName = 'file4'
    lfn3 = os.path.join( dirpath2, fileName )
    fileTuple = ( lfn3, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001' )
    res = dm.registerFile( fileTuple )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn3][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn3]['TSCatalog'], False )

    # ## Verify that the third file has not been added to the the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    for ires in res['Value']:
      self.assertNotEqual( ires['LFN'], lfn3 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # ## Create another transformation having a query not matching none of the files added to the DFC
    MDdict3 = {'particle':'gamma', 'zenith':60}
    mqJson3 = json.dumps( MDdict3 )
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', mqJson3 )
    self.assert_( res['OK'] )
    transID = res['Value']

# ## Verify that no files have been added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assertEqual( len( res['Value'] ) , 0 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )
############################

    # ## Create another transformation having an empty query
    res = transClient.addTransformation( 'transformationName', 'description', 'longDescription', 'MCSimulation', 'Standard', 'Manual', '' )
    self.assert_( res['OK'] )
    transID = res['Value']

# ## Verify that no files have been added to the transformation
    res = transClient.getTransformationFiles( {'TransformationID':transID} )
    self.assertEqual( len( res['Value'] ) , 0 )

    # ## Delete the transformation"
    res = transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )
############################

# # Remove files from both Catalogs
    res = fc.removeFile( lfn1 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn1][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn1]['TSCatalog'], True )
    res = fc.removeFile( lfn2 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn2][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn2]['TSCatalog'], True )
    res = fc.removeFile( lfn3 )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['Successful'][lfn3][metaCatalog], True )
    self.assertEqual( res['Value']['Successful'][lfn3]['TSCatalog'], 'File did not exist' )

# # Remove directories from  both Catalogs
    dirlist = [dirpath1, dirpath2]
    res = fc.removeDirectory( dirlist )
    self.assert_( res['OK'] )

# # Remove metadata fields from DFC
    for MDField in MDFieldDict.keys():
      MDFieldType = MDFieldDict[MDField]
      res = fc.deleteMetadataField( MDField )
      self.assert_( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientTransformationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
