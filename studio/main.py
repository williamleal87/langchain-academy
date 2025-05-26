import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import MessagesState
from langchain_core.messages import HumanMessage, SystemMessage
from funcoes import obter_dados_temperatura
from langgraph.graph import START, StateGraph
from langgraph.prebuilt import tools_condition, ToolNode
from langgraph.checkpoint.memory import MemorySaver

load_dotenv()
#memory = MemorySaver()
#config = {"configurable": {"thread_id": "1"}}
tools = [obter_dados_temperatura]
llm = ChatOpenAI(model=os.getenv('MODEL'), api_key=os.getenv('OPENAI_API_KEY'))
llm_with_tools = llm.bind_tools(tools)

# System message
sys_msg = SystemMessage(content="Você é um assistente chamado Davvi e possui ferramentas para ajudar nas suas respostas.")

# Node
def assistant(state: MessagesState):
   return {"messages": [llm_with_tools.invoke([sys_msg] + state["messages"])]}

# Graph
builder = StateGraph(MessagesState)

# Define nodes: these do the work
builder.add_node("assistant", assistant)
builder.add_node("tools", ToolNode(tools))

# Define edges: these determine how the control flow moves
builder.add_edge(START, "assistant")
builder.add_conditional_edges(
    "assistant",
    # If the latest message (result) from assistant is a tool call -> tools_condition routes to tools
    # If the latest message (result) from assistant is a not a tool call -> tools_condition routes to END
    tools_condition,
)
builder.add_edge("tools", "assistant")
graph = builder.compile() #checkpointer=memory
