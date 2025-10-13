import traceback
from typing import Any, List, Optional
import os

from mcp.server.fastmcp import FastMCP
import logging
import earthaccess

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Initialize FastMCP server
mcp = FastMCP("cmr-search")

def format_dataset(feature: dict) -> str:
    """Format an alert feature into a readable string."""
    props = feature

    logger.debug(props.concept_id())

    try:
        return f"""
ConceptID: {props.concept_id()}
Description: {props.abstract()}
Shortname: {props.summary()['short-name']}
"""
    except Exception as e:
        logging.error(traceback.format_exc())
        #Currently an error in earthaccess that relies on `FileDistributionInformation` to exist will be caught here from the 'summary()' method.
        # Returning empty string.
        return ""


def format_granule(granule: dict) -> str:
    """Format a granule into a readable string."""
    try:
        # Get basic granule information - concept ID is in meta
        concept_id = granule.get('meta', {}).get('concept-id', 'Unknown')

        # Get temporal information if available
        temporal_info = ""
        try:
            temporal = granule.get("umm", {}).get("TemporalExtent", {}).get("RangeDateTime", {})
            if temporal:
                begin_date = temporal.get("BeginningDateTime", "")
                end_date = temporal.get("EndingDateTime", "")
                if begin_date and end_date:
                    temporal_info = f"Temporal: {begin_date} to {end_date}"
                elif begin_date:
                    temporal_info = f"Temporal: {begin_date}"
        except:
            pass

        # Get size information if available
        size_info = ""
        try:
            size_mb = granule.size()
            if size_mb:
                size_info = f"Size: {size_mb} MB"
        except:
            pass

        # Get data links
        links_info = ""
        try:
            data_links = granule.data_links()
            if data_links:
                if len(data_links) == 1:
                    links_info = f"Data Link: {data_links[0]}"
                else:
                    links_info = "Data Links:\n" + "\n".join([f"  - {link}" for link in data_links])
        except:
            pass

        # Format the output
        result_parts = [f"ConceptID: {concept_id}"]
        if temporal_info:
            result_parts.append(temporal_info)
        if size_info:
            result_parts.append(size_info)
        if links_info:
            result_parts.append(links_info)

        return "\n".join(result_parts)

    except Exception as e:
        logging.error(f"Error formatting granule: {traceback.format_exc()}")
        return f"ConceptID: Unknown"


@mcp.tool()
async def get_datasets(
    startdate: str = None,
    stopdate: str = None,
    daac: Optional[str] = None,
    keyword: str= None) -> str:
    """Get a list of datasets form CMR based on keywords.

    Args:
        startdate: (Optional) Start date of search request (like "2002" or "2022-03-22")
        stopdate: (Optional) Stop date of search request (like "2002" or "2022-03-22")
        daac: the daac to search, e.g. NSIDC or PODAAC
        keywords: A list of keyword arguments to search collections for.
    """
    args = {}
    if keyword is not None:
         args['keyword'] = keyword
    if daac is not None:
         args['daac'] = daac
    if startdate is not None or stopdate is not None:
         args['temporal'] = (startdate, stopdate)

    collections = earthaccess.search_datasets(count=5,  **args )
    logger.debug(len(collections))

    #alerts = [format_dataset(feature) for feature in data["features"]]
    return "\n---\n".join([format_dataset(ds) for ds in collections])


@mcp.tool()
async def get_data(
    daac: Optional[str] = None,
    short_name: Optional[str] = None,
    startdate: str = None,
    stopdate: str = None,
    keyword: str = None) -> str:
    """Get a list of granules from CMR based on collection parameters.

    Args:
        daac: The DAAC to search, e.g. NSIDC or PODAAC
        short_name: The short name of the collection/dataset to search for granules
        startdate: (Optional) Start date of search request (like "2002" or "2022-03-22")
        stopdate: (Optional) Stop date of search request (like "2002" or "2022-03-22")
        keyword: A keyword to search granules for
    """
    args = {}
    if daac is not None:
        args['daac'] = daac
    if short_name is not None:
        args['short_name'] = short_name
    if startdate is not None or stopdate is not None:
        args['temporal'] = (startdate, stopdate)

    # Search for granules instead of datasets
    granules = earthaccess.search_data(count=10, **args)
    logger.debug(f"Found {len(granules)} granules")

    return "\n---\n".join([format_granule(granule) for granule in granules])


@mcp.tool()
async def download(
    local_path: str = "./downloads",
    daac: Optional[str] = None,
    short_name: Optional[str] = None,
    concept_ids: Optional[str] = None,
    startdate: str = None,
    stopdate: str = None,
    count: int = 5) -> str:
    """Download granules from CMR to a local directory using earthaccess.

    Args:
        local_path: Local directory path where files will be downloaded (default: "./downloads")
        daac: The DAAC to search, e.g. NSIDC or PODAAC
        short_name: The short name of the collection/dataset to search for granules
        concept_ids: Comma-separated list of granule concept IDs to download directly
        startdate: (Optional) Start date of search request (like "2002" or "2022-03-22")
        stopdate: (Optional) Stop date of search request (like "2002" or "2022-03-22")
        count: Maximum number of granules to download (default: 5)
    """
    try:
        # Authenticate with earthaccess
        auth = earthaccess.login()
        if not auth.authenticated:
            return "Error: Failed to authenticate with earthaccess. Please check your credentials."

        granules = []

        # If concept IDs are provided, search by concept IDs
        if concept_ids:
            concept_id_list = [cid.strip() for cid in concept_ids.split(',')]
            logger.debug(f"Searching for granules by concept IDs: {concept_id_list}")

            for concept_id in concept_id_list:
                try:
                    granule_results = earthaccess.search_data(concept_id=concept_id)
                    granules.extend(granule_results)
                except Exception as e:
                    logger.error(f"Error searching for concept ID {concept_id}: {e}")
        else:
            # Search for granules using other parameters
            args = {}
            if daac is not None:
                args['daac'] = daac
            if short_name is not None:
                args['short_name'] = short_name
            if startdate is not None or stopdate is not None:
                args['temporal'] = (startdate, stopdate)

            if not args:
                return "Error: Either concept_ids or search parameters (daac, short_name) must be provided."

            logger.debug(f"Searching for granules with parameters: {args}")
            granules = earthaccess.search_data(count=count, **args)

        if not granules:
            return "No granules found matching the specified criteria."

        # Limit the number of granules if not using concept IDs
        if not concept_ids and len(granules) > count:
            granules = granules[:count]

        logger.debug(f"Found {len(granules)} granules to download")

        # Create local directory if it doesn't exist
        os.makedirs(local_path, exist_ok=True)

        # Download the granules
        downloaded_files = earthaccess.download(granules, local_path)

        if not downloaded_files:
            return f"No files were downloaded. Check permissions and available space in {local_path}"

        # Format the response
        result_parts = [f"Successfully downloaded {len(downloaded_files)} files to {local_path}:"]

        # Add information about each downloaded file
        for i, (granule, file_path) in enumerate(zip(granules[:len(downloaded_files)], downloaded_files)):
            try:
                concept_id = granule.concept_id() if hasattr(granule, 'concept_id') else f"granule_{i}"
                file_name = os.path.basename(file_path) if isinstance(file_path, str) else f"file_{i}"
                file_size = ""
                try:
                    if isinstance(file_path, str) and os.path.exists(file_path):
                        size_bytes = os.path.getsize(file_path)
                        size_mb = size_bytes / (1024 * 1024)
                        file_size = f" ({size_mb:.2f} MB)"
                except:
                    pass

                result_parts.append(f"  - {concept_id}: {file_name}{file_size}")
            except Exception as e:
                logger.error(f"Error formatting download result for granule {i}: {e}")
                result_parts.append(f"  - Downloaded file {i}")

        return "\n".join(result_parts)

    except Exception as e:
        logger.error(f"Error in download function: {traceback.format_exc()}")
        return f"Error downloading files: {str(e)}"


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
