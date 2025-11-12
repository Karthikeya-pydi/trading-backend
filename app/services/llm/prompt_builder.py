"""
Prompt Builder for LLM Service

Builds system prompts and context-aware prompts for the trading assistant.
"""

from typing import List, Optional, Dict
from loguru import logger


class PromptBuilder:
    """Build prompts for LLM trading assistant"""
    
    SYSTEM_PROMPT = """You are an expert trading assistant for the Indian stock market. Your role is to help traders make informed decisions by analyzing their portfolio, stock returns, and market data.

IMPORTANT: You MUST use the provided trading data in your response. Do NOT use generic or outdated information. Always base your analysis on the actual data provided in the context.

Guidelines:
1. ALWAYS analyze the provided data first - if trading data is provided, use it for your analysis
2. Provide clear, actionable insights based on the ACTUAL provided data
3. Focus on risk management and portfolio optimization
4. Explain your reasoning in simple terms
5. Highlight important metrics (P&L, returns, raw scores, prices, volumes) from the data
6. Suggest portfolio adjustments when appropriate
7. Be concise but thorough
8. Always consider the trader's perspective and risk tolerance

Tone: Professional, helpful, and trader-focused
Language: Use clear English with Indian market terminology (e.g., "lakhs", "crores" for large amounts)

When analyzing data:
- Use the ACTUAL prices, volumes, and metrics from the provided data
- Compare current performance with historical returns IF data is provided
- Identify trends and patterns from the ACTUAL data
- Point out potential risks or opportunities based on the data
- Suggest concrete actions when relevant
- Explain technical terms when used
- If market data is provided, analyze specific stocks and their performance
- If portfolio data is provided, analyze the actual holdings and their P&L

CRITICAL: 
- If bhavcopy data is provided, analyze the ACTUAL stock prices, volumes, and changes
- If returns data is provided, analyze the ACTUAL returns and raw scores
- If portfolio data is provided, analyze the ACTUAL holdings and performance
- Do NOT use generic market information if specific data is provided
- Always reference the actual data points in your analysis

Remember: You are a trusted advisor, not a replacement for professional financial advice."""
    
    @staticmethod
    def build_system_prompt(custom_instructions: Optional[str] = None) -> str:
        """
        Build system prompt for the LLM
        
        Args:
            custom_instructions: Optional custom instructions to append
            
        Returns:
            System prompt string
        """
        prompt = PromptBuilder.SYSTEM_PROMPT
        
        if custom_instructions:
            prompt += f"\n\nAdditional Instructions:\n{custom_instructions}"
        
        return prompt
    
    @staticmethod
    def build_user_prompt(
        user_query: str,
        data_context: Optional[str] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> str:
        """
        Build user prompt with context and history
        
        Args:
            user_query: The user's question
            data_context: Formatted trading data context
            conversation_history: Previous conversation messages
            
        Returns:
            Formatted user prompt
        """
        prompt_parts = []
        
        # Add data context if available - EMPHASIZE USING THIS DATA
        if data_context and data_context != "No trading data available for context.":
            prompt_parts.append("=== TRADING DATA CONTEXT (USE THIS DATA FOR YOUR ANALYSIS) ===")
            prompt_parts.append("IMPORTANT: Analyze the following ACTUAL trading data. Use the prices, volumes, returns, and metrics provided below.")
            prompt_parts.append("Do NOT use generic or outdated information. Base your response on this ACTUAL data.")
            prompt_parts.append("")
            prompt_parts.append(data_context)
            prompt_parts.append("")
            prompt_parts.append("--- END OF TRADING DATA ---")
            prompt_parts.append("")
            prompt_parts.append("Remember: Use the ACTUAL data above for your analysis. Reference specific stocks, prices, volumes, and metrics from the data.")
            prompt_parts.append("")
        
        # Add conversation history if available
        if conversation_history:
            prompt_parts.append("=== CONVERSATION HISTORY ===")
            for msg in conversation_history[-5:]:  # Last 5 messages
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "user":
                    prompt_parts.append(f"User: {content}")
                elif role == "assistant":
                    prompt_parts.append(f"Assistant: {content}")
            prompt_parts.append("")
        
        # Add user query
        prompt_parts.append("=== USER QUESTION ===")
        prompt_parts.append(user_query)
        prompt_parts.append("")
        if data_context and data_context != "No trading data available for context.":
            prompt_parts.append("NOTE: Please analyze the trading data provided above and answer based on the ACTUAL data, not generic information.")
        
        return "\n".join(prompt_parts)
    
    @staticmethod
    def format_conversation_history(
        messages: List[Dict],
        limit: int = 10
    ) -> List[Dict[str, str]]:
        """
        Format conversation history for prompt
        
        Args:
            messages: List of chat history messages
            limit: Maximum number of messages to include
            
        Returns:
            Formatted conversation history
        """
        if not messages:
            return []
        
        # Get last N messages
        recent_messages = messages[-limit:] if len(messages) > limit else messages
        
        formatted_history = []
        for msg in recent_messages:
            formatted_history.append({
                "role": "user",
                "content": msg.get("user_query", "")
            })
            formatted_history.append({
                "role": "assistant",
                "content": msg.get("assistant_response", "")
            })
        
        return formatted_history
    
    @staticmethod
    def build_full_prompt(
        user_query: str,
        system_instructions: Optional[str] = None,
        data_context: Optional[str] = None,
        conversation_history: Optional[List[Dict]] = None
    ) -> Dict[str, str]:
        """
        Build complete prompt for LLM
        
        Args:
            user_query: The user's question
            system_instructions: Optional custom system instructions
            data_context: Formatted trading data context
            conversation_history: Previous conversation messages
            
        Returns:
            Dictionary with 'system' and 'user' prompts
        """
        # Format conversation history
        formatted_history = None
        if conversation_history:
            formatted_history = PromptBuilder.format_conversation_history(conversation_history)
        
        # Build prompts
        system_prompt = PromptBuilder.build_system_prompt(system_instructions)
        user_prompt = PromptBuilder.build_user_prompt(
            user_query,
            data_context,
            formatted_history
        )
        
        return {
            "system": system_prompt,
            "user": user_prompt
        }

