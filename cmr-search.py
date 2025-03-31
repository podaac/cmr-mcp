import traceback
from typing import Any,  Optional

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


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
