"""
Azure LLM Service

Main service for Azure OpenAI integration and LLM responses.
Supports Azure API Management custom endpoints.
"""

from typing import Dict, List, Optional
from loguru import logger
from openai import AzureOpenAI

from app.core.config import settings
from app.services.llm.prompt_builder import PromptBuilder
from app.services.llm.data_formatter import DataFormatter


class AzureLLMService:
    """Service for Azure OpenAI LLM integration"""
    
    def __init__(self):
        """Initialize Azure OpenAI client"""
        self.client = None
        self._initialize_client()
        self.prompt_builder = PromptBuilder()
        self.data_formatter = DataFormatter()
    
    def _initialize_client(self):
        """Initialize Azure OpenAI client with custom API Management endpoint"""
        try:
            if not settings.azure_openai_api_key or not settings.azure_openai_endpoint:
                logger.warning("Azure OpenAI credentials not configured")
                return
            
            if not settings.azure_openai_deployment_name:
                logger.warning("Azure OpenAI deployment name not configured")
                return
            
            # Build the full endpoint URL for Azure API Management
            # Endpoint format: https://oab-sophius-devtest-01.azure-api.net/karthikeya.chowdary/v1/openai/deployments/{deployment-id}/chat/completions?api-version={api-version}
            # The base endpoint should be: https://oab-sophius-devtest-01.azure-api.net/karthikeya.chowdary/v1
            # AzureOpenAI will automatically append: /openai/deployments/{deployment}/chat/completions?api-version={api_version}
            base_endpoint = settings.azure_openai_endpoint.rstrip('/')
            deployment_name = settings.azure_openai_deployment_name
            
            # Initialize Azure OpenAI client
            # AzureOpenAI expects azure_endpoint to be the base URL without /openai
            # It will construct: {azure_endpoint}/openai/deployments/{deployment}/chat/completions?api-version={api_version}
            # So for the custom API Management endpoint, we use the base URL: https://oab-sophius-devtest-01.azure-api.net/karthikeya.chowdary/v1
            self.client = AzureOpenAI(
                api_key=settings.azure_openai_api_key,
                api_version=settings.azure_openai_api_version,
                azure_endpoint=base_endpoint
            )
            
            # Store deployment name for use in API calls
            self.deployment_name = deployment_name
            
            logger.info(f"Azure OpenAI client initialized successfully with Azure API Management endpoint")
            logger.info(f"Azure Endpoint: {base_endpoint}")
            logger.info(f"Deployment: {deployment_name}, API Version: {settings.azure_openai_api_version}")
            logger.info(f"Full URL will be: {base_endpoint}/openai/deployments/{deployment_name}/chat/completions?api-version={settings.azure_openai_api_version}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Azure OpenAI client: {e}")
            self.client = None
            self.deployment_name = None
    
    def is_available(self) -> bool:
        """Check if LLM service is available"""
        return self.client is not None
    
    async def get_chat_response(
        self,
        user_query: str,
        portfolio_data: Optional[Dict] = None,
        returns_data: Optional[Dict] = None,
        bhavcopy_data: Optional[Dict] = None,
        conversation_history: Optional[List[Dict]] = None,
        system_instructions: Optional[str] = None
    ) -> Dict:
        """
        Get chat response from Azure OpenAI
        
        Args:
            user_query: User's question
            portfolio_data: Portfolio/holdings data
            returns_data: Returns data
            bhavcopy_data: Bhavcopy data
            conversation_history: Previous conversation messages
            system_instructions: Optional custom system instructions
            
        Returns:
            Dictionary with response and metadata
        """
        if not self.is_available():
            return {
                "status": "error",
                "message": "LLM service is not available. Please check Azure OpenAI configuration.",
                "response": None
            }
        
        try:
            # Format data for LLM
            portfolio_formatted = None
            returns_formatted = None
            bhavcopy_formatted = None
            
            if portfolio_data:
                portfolio_formatted = self.data_formatter.format_portfolio_for_llm(portfolio_data)
                # Log what was formatted for debugging
                if portfolio_formatted and portfolio_formatted != "Portfolio data not available.":
                    logger.info(f"Portfolio data formatted successfully. Length: {len(portfolio_formatted)} chars")
                else:
                    logger.warning(f"Portfolio data formatting failed or returned empty: {portfolio_formatted}")
            
            if returns_data:
                # Extract symbols from portfolio if available
                symbols = None
                if portfolio_data and portfolio_data.get("type") == "success":
                    result = portfolio_data.get("result", {})
                    # Try to extract symbols from nested structure
                    if isinstance(result, dict):
                        rms_holdings = result.get("RMSHoldings", {})
                        if isinstance(rms_holdings, dict):
                            holdings_dict = rms_holdings.get("Holdings", {})
                            if isinstance(holdings_dict, dict):
                                holdings = list(holdings_dict.values())
                            elif isinstance(holdings_dict, list):
                                holdings = holdings_dict
                            else:
                                holdings = []
                            
                            symbols = [
                                h.get("TradingSymbol") or h.get("Symbol") 
                                for h in holdings 
                                if isinstance(h, dict) and (h.get("TradingSymbol") or h.get("Symbol"))
                            ]
                
                returns_formatted = self.data_formatter.format_returns_for_llm(
                    returns_data, 
                    symbols
                )
                if returns_formatted and returns_formatted != "Returns data not available.":
                    logger.info(f"Returns data formatted successfully. Length: {len(returns_formatted)} chars")
            
            if bhavcopy_data:
                # Extract symbols from portfolio if available
                symbols = None
                if portfolio_data and portfolio_data.get("type") == "success":
                    result = portfolio_data.get("result", {})
                    # Try to extract symbols from nested structure
                    if isinstance(result, dict):
                        rms_holdings = result.get("RMSHoldings", {})
                        if isinstance(rms_holdings, dict):
                            holdings_dict = rms_holdings.get("Holdings", {})
                            if isinstance(holdings_dict, dict):
                                holdings = list(holdings_dict.values())
                            elif isinstance(holdings_dict, list):
                                holdings = holdings_dict
                            else:
                                holdings = []
                            
                            symbols = [
                                h.get("TradingSymbol") or h.get("Symbol") 
                                for h in holdings 
                                if isinstance(h, dict) and (h.get("TradingSymbol") or h.get("Symbol"))
                            ]
                
                bhavcopy_formatted = self.data_formatter.format_bhavcopy_for_llm(
                    bhavcopy_data,
                    symbols
                )
            
            # Combine data context
            data_context = self.data_formatter.combine_data_context(
                portfolio_formatted,
                returns_formatted,
                bhavcopy_formatted
            )
            
            # Log context summary
            logger.info(f"Data context created. Length: {len(data_context)} chars")
            if portfolio_formatted and portfolio_formatted != "Portfolio data not available.":
                logger.info("Portfolio data included in context")
            else:
                logger.warning("Portfolio data NOT included in context")
            
            if returns_formatted and returns_formatted != "Returns data not available.":
                logger.info("Returns data included in context")
            else:
                logger.warning("Returns data NOT included in context")
            
            if bhavcopy_formatted and bhavcopy_formatted != "Bhavcopy data not available.":
                logger.info("Bhavcopy data included in context")
                logger.debug(f"Bhavcopy data preview: {bhavcopy_formatted[:200]}...")
            else:
                logger.warning("Bhavcopy data NOT included in context")
            
            # Log full context for debugging (first 500 chars)
            logger.debug(f"Full data context preview: {data_context[:500]}...")
            
            # Build prompts
            prompts = self.prompt_builder.build_full_prompt(
                user_query,
                system_instructions,
                data_context,
                conversation_history
            )
            
            # Prepare messages for OpenAI API format
            messages = [
                {"role": "system", "content": prompts["system"]},
                {"role": "user", "content": prompts["user"]}
            ]
            
            # Call LLM using OpenAI client
            logger.info(f"Calling Azure OpenAI for user query: {user_query[:50]}...")
            
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=messages,
                temperature=settings.llm_temperature,
                max_tokens=settings.llm_max_tokens
            )
            
            # Extract response text
            response_text = response.choices[0].message.content
            
            # Log token usage
            usage = response.usage
            logger.info(
                f"Token usage: {usage.total_tokens} tokens "
                f"(prompt: {usage.prompt_tokens}, completion: {usage.completion_tokens})"
            )
            
            return {
                "status": "success",
                "response": response_text,
                "message": "Response generated successfully",
                "metadata": {
                    "model": self.deployment_name,
                    "temperature": settings.llm_temperature,
                    "max_tokens": settings.llm_max_tokens,
                    "tokens_used": usage.total_tokens,
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting LLM response: {e}")
            return {
                "status": "error",
                "message": f"Failed to generate response: {str(e)}",
                "response": None
            }
    
    async def get_simple_response(self, user_query: str) -> Dict:
        """
        Get simple response without data context
        
        Args:
            user_query: User's question
            
        Returns:
            Dictionary with response
        """
        return await self.get_chat_response(user_query)

