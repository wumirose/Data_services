
import os
import argparse
import yaml
import datetime

from Common.utils import LoggingUtil
from Common.kgx_file_normalizer import KGXFileNormalizer, NormalizationBrokenError, NormalizationFailedError
from Common.metadata_manager import MetadataManager as Metadata
from Common.loader_interface import SourceDataBrokenError, SourceDataFailedError
from Common.supplementation import SequenceVariantSupplementation, SupplementationFailedError
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

    def __init__(self,
                 storage_dir: str = None,
                 test_mode: bool = False,
                 source_subset: list = None):

        self.logger = LoggingUtil.init_logging("Data_services.Common.SourceDataLoadManager",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.test_mode = test_mode

        # locate and verify the main storage directory
        self.init_storage_dir(storage_dir)

        # load the config which sets up information about the data sources
        self.sources_without_strict_normalization = []
        self.load_config()

        # if there is a subset specified with the command line override the master source list
        if source_subset:
            self.source_list = source_subset

        self.logger.info(f'Active Source list: {self.source_list}\n')

        # set up the individual subdirectories for each data source
        self.init_source_dirs()

        # dict of data_source_id -> MetadataManager object
        self.metadata = {}

        # dict of data_source_id -> latest source version (to prevent double lookups)
        self.new_version_lookup = {}

        # load any existing metadata found in storage
        self.load_previous_metadata()

    def start(self):
        work_to_do, source_id = self.find_work_to_do()
        while work_to_do:
            work_to_do(source_id)
            work_to_do, source_id = self.find_work_to_do()
        self.logger.info(f'Work complete!')

    def find_work_to_do(self):

        self.logger.info(f'Checking for sources to update...')
        source_id = self.find_a_source_to_update()
        if source_id:
            return self.update_source, source_id

        self.logger.info(f'No more sources to update.. Checking for sources to normalize...')
        source_id = self.find_a_source_to_normalize()
        if source_id:
            return self.normalize_source, source_id

        self.logger.info(f'No more sources to normalize.. Checking for sources to supplement...')
        source_id = self.find_a_source_for_supplementation()
        if source_id:
            return self.supplement_source, source_id

        return None, None

    def load_previous_metadata(self):
        for source_id in self.source_list:
            self.metadata[source_id] = Metadata(source_id, self.get_source_dir_path(source_id))

    def find_a_source_to_update(self):
        for source_id in self.source_list:
            if self.check_if_source_needs_update(source_id):
                return source_id
        return None

    def check_if_source_needs_update(self, source_id):
        source_metadata = self.metadata[source_id]
        update_status = source_metadata.get_update_status()
        if update_status == Metadata.NOT_STARTED:
            return True
        elif update_status == Metadata.IN_PROGRESS:
            return False
        elif update_status == Metadata.BROKEN or update_status == Metadata.FAILED:
            # TODO do we want to retry these automatically?
            return False
        else:
            try:
                loader = source_data_loader_classes[source_id]()
                self.logger.info(f"Retrieving source version for {source_id}...")
                latest_source_version = loader.get_latest_source_version()
                if latest_source_version != source_metadata.get_source_version():
                    self.logger.info(f"Found new source version for {source_id}: {latest_source_version}")
                    source_metadata.archive_metadata()
                    self.new_version_lookup[source_id] = latest_source_version
                    return True
                else:
                    self.logger.info(f"Source version for {source_id} is up to date ({latest_source_version})")
                    return False
            except SourceDataFailedError as failed_error:
                # TODO report these by email or something automated
                self.logger.info(
                    f"SourceDataFailedError while checking for updated version for {source_id}: {failed_error.error_message}")
                source_metadata.set_version_update_error(failed_error.error_message)
                source_metadata.set_version_update_status(Metadata.FAILED)
                return False

    def update_source(self, source_id: str):
        source_metadata = self.metadata[source_id]
        source_metadata.set_update_status(Metadata.IN_PROGRESS)
        self.logger.info(f"Updating source data for {source_id}...")
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
            update_metadata = source_data_loader.load(nodes_output_file_path, edges_output_file_path)

            # update the associated metadata
            self.logger.info(f"Load finished. Updating {source_id} metadata...")
            has_sequence_variants = source_data_loader.has_sequence_variants()
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_update_info(update_metadata,
                                            update_time=current_time,
                                            has_sequence_variants=has_sequence_variants)
            source_metadata.set_update_status(Metadata.STABLE)
            source_metadata.set_normalization_status(Metadata.WAITING_ON_DEPENDENCY)
            source_metadata.set_supplementation_status(Metadata.WAITING_ON_DEPENDENCY)
            self.logger.info(f"Updating {source_id} complete.")

        except SourceDataBrokenError as broken_error:
            # TODO report these by email or something automated
            self.logger.error(f"SourceDataBrokenError while updating {source_id}: {broken_error.error_message}")
            source_metadata.set_update_error(broken_error.error_message)
            source_metadata.set_update_status(Metadata.BROKEN)

        except SourceDataFailedError as failed_error:
            # TODO report these by email or something automated
            self.logger.info(f"SourceDataFailedError while updating {source_id}: {failed_error.error_message}")
            source_metadata.set_update_error(f'{failed_error.error_message} - {failed_error.actual_error}')
            source_metadata.set_update_status(Metadata.FAILED)

        except Exception as e:
            # TODO report these by email or something automated
            source_metadata.set_update_error(repr(e))
            source_metadata.set_update_status(Metadata.FAILED)
            raise e

    def find_a_source_to_normalize(self):
        for source_id in self.source_list:
            source_metadata = self.metadata[source_id]
            # we only proceed with normalization if the latest source data update is stable
            if source_metadata.get_update_status() == Metadata.STABLE:
                normalization_status = source_metadata.get_normalization_status()
                # if we haven't attempted normalization for this source data version, queue it up
                if normalization_status == Metadata.NOT_STARTED or \
                        normalization_status == Metadata.WAITING_ON_DEPENDENCY:
                    return source_id
                elif normalization_status == Metadata.FAILED or \
                        normalization_status == Metadata.BROKEN:
                    # TODO do we want to retry these automatically?
                    pass
        return None

    def normalize_source(self, source_id: str):
        self.logger.info(f"Normalizing source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_normalization_status(Metadata.IN_PROGRESS)
        try:
            strict_normalization = False if source_id in self.sources_without_strict_normalization else True

            has_sequence_variants = source_metadata.has_sequence_variants()

            self.logger.info(f"Normalizing KGX files for {source_id}...")
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
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_normalization_info(normalization_info, normalization_time=current_time)
            source_metadata.set_normalization_status(Metadata.STABLE)
            source_metadata.set_supplementation_status(Metadata.WAITING_ON_DEPENDENCY)
            self.logger.info(f"Normalizing source {source_id} complete.")

        except NormalizationBrokenError as broken_error:
            # TODO report these by email or something automated
            error_message = f"{source_id} NormalizationBrokenError: {broken_error.error_message} - {broken_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_normalization_error(error_message)
            source_metadata.set_normalization_status(Metadata.BROKEN)
        except NormalizationFailedError as failed_error:
            # TODO report these by email or something automated
            error_message = f"{source_id} NormalizationFailedError: {failed_error.error_message} - {failed_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_normalization_error(error_message)
            source_metadata.set_normalization_status(Metadata.FAILED)
        except Exception as e:
            self.logger.error(f"Error while normalizing {source_id}: {repr(e)}")
            # TODO report these by email or something automated
            source_metadata.set_normalization_error(repr(e))
            source_metadata.set_normalization_status(Metadata.FAILED)
            raise e

    def find_a_source_for_supplementation(self):
        for source_id in self.source_list:
            if self.metadata[source_id].get_normalization_status() == Metadata.STABLE:
                supplementation_status = self.metadata[source_id].get_supplementation_status()
                if supplementation_status == Metadata.NOT_STARTED or \
                        supplementation_status == Metadata.WAITING_ON_DEPENDENCY:
                    return source_id
                elif supplementation_status == Metadata.FAILED or supplementation_status == Metadata.BROKEN:
                    # TODO do we want to retry these automatically?
                    pass
        return None

    def supplement_source(self, source_id: str):
        self.logger.info(f"Supplementing source {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_supplementation_status(Metadata.IN_PROGRESS)
        try:
            supplementation_info = {}
            if source_metadata.has_sequence_variants():
                nodes_file_path = self.get_normalized_node_file_path(source_id, source_metadata)
                supplemental_node_file_path = self.get_supplemental_node_file_path(source_id, source_metadata)
                normalized_supp_node_file_path = self.get_normalized_supp_node_file_path(source_id, source_metadata)
                supp_node_norm_failures_file_path = self.get_supp_node_norm_failures_file_path(source_id, source_metadata)
                supplemental_edge_file_path = self.get_supplemental_edge_file_path(source_id, source_metadata)
                normalized_supp_edge_file_path = self.get_normalized_supplemental_edge_file_path(source_id, source_metadata)
                supp_edge_norm_failures_file_path = self.get_supp_edge_norm_failures_file_path(source_id, source_metadata)
                sv_supp = SequenceVariantSupplementation(os.path.join(self.storage_dir, "resources"))
                supplementation_info = sv_supp.find_supplemental_data(nodes_file_path=nodes_file_path,
                                                                      supp_nodes_file_path=supplemental_node_file_path,
                                                                      normalized_supp_node_file_path=normalized_supp_node_file_path,
                                                                      supp_node_norm_failures_file_path=supp_node_norm_failures_file_path,
                                                                      supp_edges_file_path=supplemental_edge_file_path,
                                                                      normalized_supp_edge_file_path=normalized_supp_edge_file_path,
                                                                      supp_edge_norm_failures_file_path=supp_edge_norm_failures_file_path
                                                                      )
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_supplementation_info(supplementation_info, supplementation_time=current_time)
            source_metadata.set_supplementation_status(Metadata.STABLE)
            self.logger.info(f"Supplementing source {source_id} complete.")
        except SupplementationFailedError as failed_error:
            # TODO report these by email or something automated
            error_message = f"{source_id} SupplementationFailedError: " \
                            f"{failed_error.error_message} - {failed_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_supplementation_error(error_message)
            source_metadata.set_supplementation_status(Metadata.FAILED)
        except Exception as e:
            self.logger.error(f"{source_id} Error while supplementing: {repr(e)}")
            # TODO report these by email or something automated
            source_metadata.set_supplementation_error(repr(e))
            source_metadata.set_supplementation_status(Metadata.FAILED)
            raise e

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

    def get_supplemental_node_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_nodes.json')

    def get_normalized_supp_node_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_nodes.json')

    def get_supp_node_norm_failures_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_nodes_failures.log')

    def get_supplemental_edge_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_edges.json')

    def get_normalized_supplemental_edge_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_edges.json')

    def get_supp_edge_norm_failures_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_edge_failures.log')

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
        if 'DATA_SERVICES_CONFIG' in os.environ and os.environ['DATA_SERVICES_CONFIG']:
            config_file_name = os.environ['DATA_SERVICES_CONFIG']
            config_path = os.path.join(self.storage_dir, config_file_name)
        else:
            # otherwise use the default one included in the codebase
            config_path = os.path.dirname(os.path.abspath(__file__)) + '/../default-config.yml'

        with open(config_path) as config_file:
            config = yaml.full_load(config_file)
            self.source_list = []
            for data_source_config in config['data_sources']:
                data_source_id = data_source_config['id']
                self.source_list.append(data_source_id)
                if 'strict_normalization' in data_source_config:
                    if not data_source_config['strict_normalization']:
                        self.sources_without_strict_normalization.append(data_source_id)
        self.logger.debug(f'Config loaded... ({config_path})')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files.")
    parser.add_argument('-dir', '--storage', help='Specify the storage directory. The environment variable DATA_SERVICES_STORAGE is used otherwise.')
    parser.add_argument('-ds', '--data_source', default='all', help=f'Select a single data source to process from the following: {source_data_loader_classes.keys()}')
    parser.add_argument('-t', '--test_mode', action='store_true', help='Test mode will load a small sample version of the data.')
    args = parser.parse_args()

    data_source = args.data_source
    if data_source == "all":
        load_manager = SourceDataLoadManager(test_mode=args.test_mode)
        load_manager.start()
    else:
        if data_source not in source_data_loader_classes.keys():
            print(f'Data source not valid. Aborting. (Invalid source: {data_source})')
        else:
            load_manager = SourceDataLoadManager(source_subset=[data_source], test_mode=args.test_mode)
            load_manager.start()
