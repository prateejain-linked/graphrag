# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Main definition."""

import asyncio
import json
import logging
import platform
import sys
import time
import warnings
from pathlib import Path
import os

from typing import cast
from graphrag.config import (
    GraphRagConfig,
    create_graphrag_config,
)
from graphrag.config.enums import ContextSwitchType
from graphrag.common.utils.common_utils import is_valid_guid
from graphrag.index import PipelineConfig, create_pipeline_config
from graphrag.index.cache import NoopPipelineCache
from graphrag.common.progress import (
    NullProgressReporter,
    PrintProgressReporter,
    ProgressReporter,
)
from graphrag.common.progress.rich import RichProgressReporter
from graphrag.index.run import run_pipeline_with_config

from .emit import TableEmitterType
from .graph.extractors.claims.prompts import CLAIM_EXTRACTION_PROMPT
from .graph.extractors.community_reports.prompts import COMMUNITY_REPORT_PROMPT
from .graph.extractors.graph.prompts import GRAPH_EXTRACTION_PROMPT
from .graph.extractors.summarize.prompts import SUMMARIZE_PROMPT
from .init_content import INIT_DOTENV, INIT_YAML

from graphrag.vector_stores.typing import VectorStoreFactory
from dataclasses import dataclass
from hashlib import sha256
from graphrag.query.cli import _create_graphrag_config

# Ignore warnings from numba
warnings.filterwarnings("ignore", message=".*NumbaDeprecationWarning.*")

log = logging.getLogger(__name__)

def redact(input: dict) -> str:
    """Sanitize the config json."""

    # Redact any sensitive configuration
    def redact_dict(input: dict) -> dict:
        if not isinstance(input, dict):
            return input

        result = {}
        for key, value in input.items():
            if key in {
                "api_key",
                "connection_string",
                "container_name",
                "organization",
            }:
                if value is not None:
                    result[key] = f"REDACTED, length {len(value)}"
            elif isinstance(value, dict):
                result[key] = redact_dict(value)
            elif isinstance(value, list):
                result[key] = [redact_dict(i) for i in value]
            else:
                result[key] = value
        return result

    redacted_dict = redact_dict(input)
    return json.dumps(redacted_dict, indent=4)



@dataclass
class DocStat:
    id: str
    in_path: str
    out_path: str


def load_doc_stats(in_base:str,out_base:str,folders,context_id,config_args):
    in_fname='email.txt' #obviously should change later
    if not config_args:
            config_args = {}

    collection_name = ''
    config_args.update({"collection_name": collection_name})
    config_args.update({"vector_name": ''})
    config_args.update({"reports_name": ''})
    config_args.update({"text_units_name":''})
    config_args.update({"docs_tbl_name": f"docs_{context_id}"})

    kusto= VectorStoreFactory.get_vector_store(
        vector_store_type='kusto', kwargs=config_args
    )

    kusto.connect(**config_args)
    kusto.setup_docs()

    def get_file_hash(path):
        '''
            NOTE: Openning the file in text instead of bytes mode might result in 
            wrong hashes. But since graphrag does this, I'll keep it this way here as well 
            for conssitency and to get the same hashes here and in text.py.
            usedforsecurity param is also not necessary.
        '''

        f=open(path,"r",encoding='utf-8')
        c=f.read()
        r=sha256(c.encode(),usedforsecurity=False).hexdigest()
        f.close()
        return r


    doc_stats=[]

    store_fake_path=True # set to false to store actual filesystem path in kusto
    for p_id in range(len(folders)):
        in_p=f"{in_base}\\{p_id}\\{in_fname}"
        out_p=f"{out_base}\\{p_id}"

        hash=get_file_hash(in_p)

        if store_fake_path:
            in_p="INPUT_PATH"
            out_p="OUTPUT_PATH"

        doc_stats.append( DocStat(id=hash , in_path=in_p, out_path=out_p) )

    kusto.load_doc_stats(doc_stats)

    logging.info("Documents stats loaded in kusto.")
    

def index_cli(
    root: str,
    init: bool,
    community_level: int,
    context_operation: str | None,
    context_id: str | None,
    verbose: bool,
    resume: str | None,
    memprofile: bool,
    nocache: bool,
    config: str | None,
    emit: str | None,
    dryrun: bool,
    overlay_defaults: bool,
    cli: bool = False,
    use_kusto_community_reports: bool = False,
    optimized_search: bool = False,
 
):
    """Run the pipeline with the given config."""
    logging.info("** IN CLI \n\n")
    logging.info("Platform: "+sys.platform)
    #root = Path(__file__).parent.parent.parent.__str__()
    run_id = resume or time.strftime("%Y%m%d-%H%M%S")
    _enable_logging(root, run_id, verbose)
    progress_reporter = _get_progress_reporter("none")
    if init: 
        _initialize_project_at(root, progress_reporter)
    if overlay_defaults:
        pipeline_config: str | PipelineConfig = _create_default_config(
            root, config, verbose, dryrun or False, progress_reporter
        )
    else:
        pipeline_config: str | PipelineConfig = config or _create_default_config(
            root, None, verbose, dryrun or False, progress_reporter
        )

    def _run_workflow_async() -> None:
        import signal

        def handle_signal(signum, _):
            # Handle the signal here
            progress_reporter.info(f"Received signal {signum}, exiting...")
            progress_reporter.dispose()
            for task in asyncio.all_tasks():
                task.cancel()
            progress_reporter.info("All tasks cancelled. Exiting...")

        # Register signal handlers for SIGINT and SIGHUP
        #signal.signal(signal.SIGINT, handle_signal)

        if sys.platform != "win32":
            signal.signal(signal.SIGHUP, handle_signal)

        async def execute():
            nonlocal encountered_errors
            async for output in run_pipeline_with_config(
                pipeline_config,
                run_id=run_id,
                memory_profile=memprofile,
                cache=cache,
                progress_reporter=progress_reporter,
                emit=(
                    [TableEmitterType(e) for e in pipeline_emit]
                    if pipeline_emit
                    else None
                ),
                is_resume_run=bool(resume),
                context_id=context_id,
            ):
                if output.errors and len(output.errors) > 0:
                    encountered_errors = True
                    progress_reporter.error(output.workflow)
                else:
                    progress_reporter.success(output.workflow)

                progress_reporter.info(str(output.result))

        if platform.system() == "Windows":
            import nest_asyncio  # type: ignore Ignoring because out of windows this will cause an error

            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(execute())
        elif sys.version_info >= (3, 11):
            import uvloop  # type: ignore Ignoring because on windows this will cause an error

            with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:  # type: ignore Ignoring because minor versions this will throw an error
                runner.run(execute())
        else:
            import uvloop  # type: ignore Ignoring because on windows this will cause an error

            uvloop.install()
            asyncio.run(execute())

    if not context_id:
        logging.error('Must pass context_id')
        exit(-1)
        
    ################ CONTEXT SWITCHING
    if context_operation:
        #if not is_valid_guid(context_id):
         #   ValueError("ContextId is invalid: It should be a valid Guid")
        if (context_operation != ContextSwitchType.Activate and context_operation != ContextSwitchType.Deactivate):
            raise ValueError("ContextOperation is invalid: It should be activate or deactivate")
        _switch_context(
                root=root,
                config=config,
                reporter=progress_reporter,
                context_operation=context_operation,
                context_id=context_id,
                community_level=community_level,
                optimized_search=optimized_search,
                use_kusto_community_reports=use_kusto_community_reports,
            )
        return
    

    ################ INDEXING ITERATION
    orig_storage_base=pipeline_config.storage.base_dir
    orig_input_base=" "
    #input_full_path = os.path.dirname(__file__) + '\\..\\.' + root + "\\" + pipeline_config.input.base_dir
    #i_count=len(os.listdir(root+"\\input"))
    input_full_path = " "
    folders=os.listdir(input_full_path)
    i_count=len(folders)

    logging.info("** indexer loop")
    batch_size=50
    batch_stat_f="batch_index.txt"
    if not os.path.exists(batch_stat_f):
        f=open(batch_stat_f,"w")
        f.close()
    
    f=open(batch_stat_f,"r+")
    f.seek(0,0)
    c=f.read()
    if len(c)>0:
        batch_f_index=int(c)
    else:
        batch_f_index=0
    
    logging.info("Current batch: "+ str(batch_f_index))
    end = min(i_count,batch_f_index+batch_size)
    i_start=batch_f_index

    if i_start==0:
        logging.info("Loading document stats in kusto")
        cc=_create_graphrag_config( root,config) #from query module
        load_doc_stats(input_full_path,orig_storage_base,folders,context_id,cc.embeddings.vector_store)

    for p_id in range(i_start,end):

        pipeline_config.input.base_dir=f'{orig_input_base}\\{p_id}'
        pipeline_config.storage.base_dir = f'{orig_storage_base}\\{p_id}'

        #pipeline_config.input.base_dir=f'{orig_input_base}/{folders[p_id]}'
        #pipeline_config.storage.base_dir = f'{orig_storage_base}/{p_id}'

        logging.info("Working with input "+pipeline_config.input.base_dir)
        logging.info("out "+pipeline_config.storage.base_dir)
        
        cache = NoopPipelineCache() if nocache else None
        pipeline_emit = emit.split(",") if emit else None
        encountered_errors = False

        

        _run_workflow_async()
        progress_reporter.stop()
        if encountered_errors:
            progress_reporter.error(
                "Errors occurred during the pipeline run, see logs for more details."
            )
            logging.info("[!] Got errors")
            #exit(-1)
        else:
            progress_reporter.success("All workflows completed successfully.")
            
            
            batch_f_index += 1
            logging.info("Updating batch file index: "+str(batch_f_index))
            f.seek(0,0)
            f.write(str(batch_f_index))
    
    f.close()



def _switch_context(root: str, config: str,
                    reporter: ProgressReporter, context_operation: str | None,
                    context_id: str, community_level: int, optimized_search: bool,
                    use_kusto_community_reports: bool) -> None:
    """Switch the context to the given context."""
    reporter.info(f"Switching context to {context_id} using operation {context_operation}")
    logging.info("Switching context to {context_id}")
    from graphrag.index.context_switch.contextSwitcher import ContextSwitcher
    context_switcher = ContextSwitcher(
        root_dir=root,
        config_dir=config,
        reporter=reporter,
        context_id=context_id,
        community_level=community_level,
        data_dir=None,
        optimized_search=optimized_search,
        use_kusto_community_reports=use_kusto_community_reports)
    if context_operation == ContextSwitchType.Activate:
        context_switcher.activate()
    elif context_operation == ContextSwitchType.Deactivate:
        context_switcher.deactivate()
    else:
        msg = f"Invalid context operation {context_operation}"
        raise ValueError(msg)

def _initialize_project_at(path: str, reporter: ProgressReporter) -> None:
    """Initialize the project at the given path."""
    reporter.info(f"Initializing project at {path}")
    root = Path(path)
    if not root.exists():
        root.mkdir(parents=True, exist_ok=True)

    settings_yaml = root / "settings/settings.yaml"
    
    dotenv = root / ".env"
    if not dotenv.exists():
        with settings_yaml.open("wb") as file:
            file.write(INIT_YAML.encode(encoding="utf-8", errors="strict"))

    '''
    with dotenv.open("wb") as file:
        file.write(INIT_DOTENV.encode(encoding="utf-8", errors="strict"))
    '''

    prompts_dir = root / "prompts"
    if not prompts_dir.exists():
        prompts_dir.mkdir(parents=True, exist_ok=True)

    entity_extraction = prompts_dir / "entity_extraction.txt"
    if not entity_extraction.exists():
        with entity_extraction.open("wb") as file:
            file.write(
                GRAPH_EXTRACTION_PROMPT.encode(encoding="utf-8", errors="strict")
            )

    summarize_descriptions = prompts_dir / "summarize_descriptions.txt"
    if not summarize_descriptions.exists():
        with summarize_descriptions.open("wb") as file:
            file.write(SUMMARIZE_PROMPT.encode(encoding="utf-8", errors="strict"))

    claim_extraction = prompts_dir / "claim_extraction.txt"
    if not claim_extraction.exists():
        with claim_extraction.open("wb") as file:
            file.write(
                CLAIM_EXTRACTION_PROMPT.encode(encoding="utf-8", errors="strict")
            )

    community_report = prompts_dir / "community_report.txt"
    if not community_report.exists():
        with community_report.open("wb") as file:
            file.write(
                COMMUNITY_REPORT_PROMPT.encode(encoding="utf-8", errors="strict")
            )


def _create_default_config(
    root: str,
    config: str | None,
    verbose: bool,
    dryrun: bool,
    reporter: ProgressReporter,
) -> PipelineConfig:
    """Overlay default values on an existing config or create a default config if none is provided."""
    if config and not Path(config).exists():
        msg = f"Configuration file {config} does not exist"
        raise ValueError

    if not Path(root).exists():
        msg = f"Root directory {root} does not exist"
        raise ValueError(msg)

    parameters = _read_config_parameters(root, config, reporter)
    #log.info(
    #    "using default configuration: %s",
    #    redact(parameters.model_dump()),
    #)

    if verbose or dryrun:
        reporter.info(f"Using default configuration: {redact(parameters.model_dump())}")
    result = create_pipeline_config(parameters, verbose)
    if verbose or dryrun:
        reporter.info(f"Final Config: {redact(result.model_dump())}")

    if dryrun:
        reporter.info("dry run complete, exiting...")
        sys.exit(0)
    return result


def _read_config_parameters(root: str, config: str | None, reporter: ProgressReporter):
    _root = Path(root)
    settings_yaml = (
        Path(config)
        if config and Path(config).suffix in [".yaml", ".yml"]
        else _root / "settings.yaml"
    )
    if not settings_yaml.exists():
        settings_yaml = _root / "settings.yml"
    settings_json = (
        Path(config)
        if config and Path(config).suffix == ".json"
        else _root / "settings.json"
    )

    if settings_yaml.exists():
        reporter.success(f"Reading settings from {settings_yaml}")
        with settings_yaml.open("rb") as file:
            import yaml

            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            return create_graphrag_config(data, root)

    if settings_json.exists():
        reporter.success(f"Reading settings from {settings_json}")
        with settings_json.open("rb") as file:
            import json

            data = json.loads(file.read().decode(encoding="utf-8", errors="strict"))
            return create_graphrag_config(data, root)

    reporter.success("Reading settings from environment variables")
    return create_graphrag_config(root_dir=root)


def _get_progress_reporter(reporter_type: str | None) -> ProgressReporter:
    if reporter_type is None or reporter_type == "rich":
        return RichProgressReporter("GraphRAG Indexer ")
    if reporter_type == "print":
        return PrintProgressReporter("GraphRAG Indexer ")
    if reporter_type == "none":
        return NullProgressReporter()

    msg = f"Invalid progress reporter type: {reporter_type}"
    raise ValueError(msg)


def _enable_logging(root_dir: str, run_id: str, verbose: bool) -> None:
    logging_file = (
        Path(root_dir) / "output" / run_id / "reports" / "indexing-engine.log"
    )
    logging_file.parent.mkdir(parents=True, exist_ok=True)

    logging_file.touch(exist_ok=True)
    handler = logging.StreamHandler(stream=sys.stdout)
    fileHandler = logging.FileHandler(logging_file, mode="a")
    logging.basicConfig(
        #filename=str(logging_file),
        #filemode="a",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG if verbose else logging.INFO,
        handlers=[handler, fileHandler]
    )


