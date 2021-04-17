
import os
import multiprocessing
import argparse
import yaml

from Common.utils import LoggingUtil
from Common.kgx_file_normalizer import KGXFileNormalizer, NormalizationBrokenError, NormalizationFailedError
from Common.metadata_manager import MetadataManager as Metadata
from Common.loader_interface import SourceDataBrokenError, SourceDataFailedError
from GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader
from CTD.src.loadCTD import CTDLoader
from FooDB.src.loadFDB import FDBLoader
from GOA.src.loadGOA import GOALoader
from IntAct.src.loadIA import IALoader
from PHAROS.src.loadPHAROS import PHAROSLoader
from UberGraph.src.loadUG import UGLoader
from ViralProteome.src.loadVP import VPLoader
from ViralProteome.src.loadUniRef import UniRefSimLoader
from gtopdb.src.loadGtoPdb import GtoPdbLoader
from hmdb.src.loadHMDB import HMDBLoader
from hgnc.src.loadHGNC import HGNCLoader
#from panther.src.loadPanther import PLoader

GWAS_CATALOG = 'GWASCatalog'
CTD = 'CTD'
FOODB = 'FooDB' # this is on hold, data needs review after latest release of data.
HUMAN_GOA = 'HumanGOA' # this has normalization issues (needs pre-norm to create edges)
INTACT = "IntAct"
PHAROS = 'PHAROS'
UBERGRAPH = 'UberGraph'
UNIREF = "UniRef"
VP = 'ViralProteome'
GTOPDB = 'GtoPdb'
HMDB = 'HMDB'
HGNC = 'HGNC'
PANTHER = 'PANTHER'

ALL_SOURCES = [
    CTD,
    INTACT,
    GTOPDB,
    HUMAN_GOA,
    HGNC,
    UBERGRAPH,
    VP,
    HMDB,
    GWAS_CATALOG

    # in progress
    # PANTHER,

    # items to go
    # biolink,
    # chembio,
    # chemnorm,
    # cord19-scibite,
    # cord19-scigraph,
    # covid-phenotypes,
    # hetio,
    # kegg,
    # mychem,
    # ontological-hierarchy,
    # textminingkp,

    # items with issues
    # PHAROS - normalization issues in load manager. normalization lists are too large to parse.
    # FOODB - no longer has curies that will normalize.
    # UNIREF - normalization issues in load manager. normalization lists are too large to parse.
]

source_data_loader_classes = {
    CTD: CTDLoader,
    INTACT: IALoader,
    GTOPDB: GtoPdbLoader,
    HUMAN_GOA: GOALoader,
    HGNC: HGNCLoader,
    UBERGRAPH: UGLoader,
    VP: VPLoader,
    HMDB: HMDBLoader,
    GWAS_CATALOG: GWASCatalogLoader

    # in progress
    # PANTHER: PLoader,

    # items to go
    # biolink,
    # chembio,
    # chemnorm,
    # cord19-scibite,
    # cord19-scigraph,
    # covid-phenotypes,
    # hetio,
    # kegg,
    # mychem,
    # ontological-hierarchy,
    # textminingkp,

    # items with issues
    # PHAROS: PHAROSLoader - normalization issues in load manager. normalization lists are too large to parse.
    # FOODB: FDBLoader - no longer has curies that will normalize
    # UNIREF: UniRefSimLoader - normalization issues in load manager. normalization lists are too large to parse.
}


class SourceDataLoadManager:

    logger = LoggingUtil.init_logging("Data_services.Common.SourceDataLoadManager",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self,
                 storage_dir: str = None,
                 test_mode: bool = False,
                 source_subset: list = None):

        self.test_mode = test_mode

        # locate and verify the main storage directory
        self.init_storage_dir(storage_dir)

        # load the config which sets up information about the data sources
        self.sources_without_strict_normalization = []
        self.sources_with_variants = []
        self.load_config()

        # if there is a subset specified with the command line override the master source list
        if source_subset:
            self.source_list = source_subset

        # set up the individual subdirectories for each data source
        self.init_source_dirs()

        # dict of data_source_id -> MetadataManager object
        self.metadata = {}

        # dict of data_source_id -> latest source version (to prevent double lookups)
        self.new_version_lookup = {}

        # load any existing metadata found in storage
        self.load_previous_metadata()

    def start(self):

        # TODO determine multiprocessing pool size by deployment capabilities
        pool_size = 6

        self.logger.info(f'Checking for sources to update...')
        sources_to_update = self.check_sources_for_updates()
        self.logger.info(f'Updating {len(sources_to_update)} sources: {repr(sources_to_update)}')
        update_func = self.update_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(update_func, sources_to_update)
        pool.close()

        self.logger.info(f'Checking for sources to normalize...')
        sources_to_normalize = self.check_sources_for_normalization()
        self.logger.info(f'Normalizing {len(sources_to_normalize)} sources: {repr(sources_to_normalize)}')
        # TODO can we really do this in parallel or will the normalization services barf?
        normalize_func = self.normalize_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(normalize_func, sources_to_normalize)
        pool.close()

        self.logger.info(f'Checking for sources to annotate...')
        sources_to_annotate = self.check_sources_for_annotation()
        self.logger.info(f'Annotating {len(sources_to_annotate)} sources: {repr(sources_to_annotate)}')
        annotate_func = self.annotate_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(annotate_func, sources_to_annotate)
        pool.close()

    def load_previous_metadata(self):
        for source_id in self.source_list:
            self.metadata[source_id] = Metadata(source_id, self.get_source_dir_path(source_id))

    def check_sources_for_updates(self):
        sources_to_update = []
        for source_id in self.source_list:
            source_metadata = self.metadata[source_id]
            update_status = source_metadata.get_update_status()
            if update_status == Metadata.NOT_STARTED:
                sources_to_update.append(source_id)
            elif update_status == Metadata.IN_PROGRESS:
                continue
            elif update_status == Metadata.BROKEN:
                pass
            elif update_status == Metadata.FAILED:
                pass
                # TODO do we want to retry these automatically?

            else:
                try:
                    loader = source_data_loader_classes[source_id]()
                    self.logger.info(f"Retrieving source version for {source_id}...")
                    latest_source_version = loader.get_latest_source_version()
                    if latest_source_version != source_metadata.get_source_version():
                        self.logger.info(f"Found new source version for {source_id}: {latest_source_version}")
                        source_metadata.archive_metadata()
                        sources_to_update.append(source_id)
                        self.new_version_lookup[source_id] = latest_source_version
                    else:
                        self.logger.info(f"Source version for {source_id} is up to date ({latest_source_version})")
                except SourceDataFailedError as failed_error:
                    # TODO report these by email or something automated
                    self.logger.info(
                        f"SourceDataFailedError while checking for updated version for {source_id}: {failed_error.error_message}")
                    # TODO there isn't currently a good spot to indicate an error here in the metadata
                    #source_metadata.set_version_update_error(failed_error.error_message)
                    #source_metadata.set_version_update_status(Metadata.FAILED)

        return sources_to_update

    def update_source(self, source_id: str):
        self.logger.info(f"Updating source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_update_status(Metadata.IN_PROGRESS)
        try:
            # create an instance of the appropriate loader using the source_data_loader_classes lookup map
            source_data_loader = source_data_loader_classes[source_id](test_mode=self.test_mode)
            # update the version and load information
            if source_id in self.new_version_lookup:
                latest_source_version = self.new_version_lookup[source_id]
            else:
                self.logger.info(f"Retrieving source version for {source_id}...")
                latest_source_version = source_data_loader.get_latest_source_version()
                self.logger.info(f"Found new source version for {source_id}: {latest_source_version}")

            source_metadata.update_version(latest_source_version)
            # call the loader - retrieve/parse data and write to a kgx file
            self.logger.info(f"Loading new version of {source_id} ({latest_source_version})...")
            nodes_output_file_path = self.get_source_node_file_path(source_id, source_metadata)
            edges_output_file_path = self.get_source_edge_file_path(source_id, source_metadata)
            load_meta_data = source_data_loader.load(nodes_output_file_path, edges_output_file_path)

            # update the associated metadata
            self.logger.info(f"Load finished. Updating {source_id} metadata...")
            source_metadata.set_update_status(Metadata.STABLE)
            source_metadata.set_update_info(load_meta_data)
            self.logger.info(f"Updating {source_id} complete.")

        except SourceDataBrokenError as broken_error:
            # TODO report these by email or something automated
            self.logger.error(f"SourceDataBrokenError while updating {source_id}: {broken_error.error_message}")
            source_metadata.set_update_error(broken_error.error_message)
            source_metadata.set_update_status(Metadata.BROKEN)

        except SourceDataFailedError as failed_error:
            # TODO report these by email or something automated
            self.logger.info(f"SourceDataFailedError while updating {source_id}: {failed_error.error_message}")
            source_metadata.set_update_error(failed_error.error_message)
            source_metadata.set_update_status(Metadata.FAILED)

        except Exception as e:
            # TODO report these by email or something automated
            source_metadata.set_update_error(repr(e))
            source_metadata.set_update_status(Metadata.FAILED)
            raise e

    def check_sources_for_normalization(self):
        sources_to_normalize = []
        for source_id in self.source_list:
            source_metadata = self.metadata[source_id]
            normalization_status = source_metadata.get_normalization_status()
            if normalization_status == Metadata.NOT_STARTED:
                sources_to_normalize.append(source_id)
            elif ((normalization_status == Metadata.WAITING_ON_DEPENDENCY)
                  and (source_metadata.get_update_status() == Metadata.STABLE)):
                sources_to_normalize.append(source_id)
            elif normalization_status == Metadata.FAILED:
                sources_to_normalize.append(source_id)
                # TODO should probably have a retry max limit
        return sources_to_normalize

    def normalize_source(self, source_id: str):
        self.logger.debug(f"Normalizing source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_normalization_status(Metadata.IN_PROGRESS)
        try:
            strict_normalization = False if source_id in self.sources_without_strict_normalization else True

            has_sequence_variants = True if source_id in self.sources_with_variants else False

            self.logger.debug(f"Normalizing KGX files for {source_id}...")
            nodes_source_file_path = self.get_source_node_file_path(source_id, source_metadata)
            nodes_norm_file_path = self.get_normalized_node_file_path(source_id, source_metadata)
            node_norm_failures_file_path = self.get_node_norm_failures_file_path(source_id, source_metadata)
            edges_source_file_path = self.get_source_edge_file_path(source_id, source_metadata)
            edges_norm_file_path = self.get_normalized_edge_file_path(source_id, source_metadata)
            edge_norm_failures_file_path = self.get_edge_norm_failures_file_path(source_id, source_metadata)
            file_normalizer = KGXFileNormalizer(nodes_source_file_path,
                                                nodes_norm_file_path,
                                                node_norm_failures_file_path,
                                                edges_source_file_path,
                                                edges_norm_file_path,
                                                edge_norm_failures_file_path,
                                                has_sequence_variants=has_sequence_variants,
                                                strict_normalization=strict_normalization)

            normalization_info = file_normalizer.normalize_kgx_files()
            # self.logger.info(f"Normalization info for {source_id}: {normalization_info}")

            # update the associated metadata
            source_metadata.set_normalization_status(Metadata.STABLE)
            source_metadata.set_normalization_info(normalization_info)
            self.logger.debug(f"Normalizing source {source_id} complete.")

        except NormalizationBrokenError as broken_error:
            # TODO report these by email or something automated
            self.logger.error(f"NormalizationBrokenError while normalizing {source_id}: {broken_error.error_message}")
            source_metadata.set_normalization_error(broken_error.error_message)
            source_metadata.set_normalization_status(Metadata.BROKEN)
        except NormalizationFailedError as failed_error:
            # TODO report these by email or something automated
            self.logger.error(f"NormalizationFailedError while normalizing {source_id}: {failed_error.error_message}")
            source_metadata.set_normalization_error(failed_error.error_message)
            source_metadata.set_normalization_status(Metadata.FAILED)
        except Exception as e:
            self.logger.error(f"Error while normalizing {source_id}: {repr(e)}")
            # TODO report these by email or something automated
            source_metadata.set_normalization_error(repr(e))
            source_metadata.set_normalization_status(Metadata.FAILED)
            raise e

    def check_sources_for_annotation(self):
        sources_to_annotate = []
        for source_id in self.source_list:
            annotation_status = self.metadata[source_id].get_annotation_status()
            if annotation_status == Metadata.NOT_STARTED:
                sources_to_annotate.append(source_id)
            elif ((annotation_status == Metadata.WAITING_ON_DEPENDENCY)
                  and (self.metadata[source_id].get_normalization_status() == Metadata.STABLE)):
                sources_to_annotate.append(source_id)
            elif annotation_status:
                # TODO - log and report errors
                pass
        return sources_to_annotate

    def annotate_source(self, source_id: str):
        pass

    @staticmethod
    def get_versioned_file_name(source_id: str, source_metadata: dict):
        return f'{source_id}_{source_metadata.get_load_version()}'

    def get_source_node_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_source_nodes.json')

    def get_source_edge_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_source_edges.json')

    def get_normalized_node_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_nodes.json')

    def get_node_norm_failures_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_node_failures.log')

    def get_normalized_edge_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_edges.json')

    def get_edge_norm_failures_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_edge_failures.log')

    def get_source_dir_path(self, source_id: str):
        return os.path.join(self.storage_dir, source_id)

    def init_storage_dir(self, storage_dir: str):
        # use the storage directory specified if there is one
        if storage_dir:
            self.storage_dir = storage_dir
        else:
            # otherwise use the one specified by the environment variable
            if 'DATA_SERVICES_STORAGE' in os.environ:
                self.storage_dir = os.environ["DATA_SERVICES_STORAGE"]
            else:
                # if neither exist back out
                raise IOError('SourceDataLoadManager - specify the storage directory with environment variable DATA_SERVICES_STORAGE.')

        # make sure the storage dir is a real directory
        if not os.path.isdir(self.storage_dir):
            raise IOError(f'SourceDataLoadManager - storage directory specified is invalid ({self.storage_dir}).')

    def init_source_dirs(self):
        # for each source on the source_list make sure they have subdirectories set up
        for source_id in self.source_list:
            source_dir_path = self.get_source_dir_path(source_id)
            if not os.path.isdir(source_dir_path):
                self.logger.info(f"SourceDataLoadManager creating storage dir for {source_id}... {source_dir_path}")
                os.mkdir(source_dir_path)

    def load_config(self):
        # check for a config file name specified by the environment variable
        # a custom config file must reside at the top level of the storage directory
        if 'DATA_SERVICES_CONFIG' in os.environ:
            config_file_name = os.environ['DATA_SERVICES_CONFIG']
            config_path = os.path.join(self.storage_dir, config_file_name)
        else:
            # otherwise use the default one included in the codebase
            config_path = os.path.dirname(os.path.abspath(__file__)) + '/default-config.yml'

        with open(config_path) as config_file:
            config = yaml.full_load(config_file)
            self.source_list = []
            for data_source_config in config['data_sources']:
                data_source_id = data_source_config['id']
                self.source_list.append(data_source_id)
                if 'strict_normalization' in data_source_config:
                    if not data_source_config['strict_normalization']:
                        self.sources_without_strict_normalization.append(data_source_id)
                if 'has_sequence_variants' in data_source_config:
                    if data_source_config['has_sequence_variants']:
                        self.sources_with_variants.append(data_source_id)
            self.logger.info(f'Loaded config:\nAll sources: {self.source_list}\nSources with variants: {self.sources_with_variants}\nSources without Strict Norm: {self.sources_without_strict_normalization}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files.")
    parser.add_argument('-dir', '--storage', help='Specify the storage directory. The environment variable DATA_SERVICES_STORAGE is used otherwise.')
    parser.add_argument('-ds', '--data_source', default='all', help=f'Select a single data source to process from the following: {ALL_SOURCES}')
    parser.add_argument('-t', '--test_mode', action='store_true', help='Test mode will load a small sample version of the data.')
    args = parser.parse_args()

    data_source = args.data_source
    if data_source == "all":
        load_manager = SourceDataLoadManager(test_mode=args.test_mode)
        load_manager.start()
    else:
        if data_source not in ALL_SOURCES:
            print(f'Data source not valid. Aborting. (Invalid source: {data_source})')
        else:
            load_manager = SourceDataLoadManager(source_subset=[data_source], test_mode=args.test_mode)
            load_manager.start()
