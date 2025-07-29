import chainlit as cl
import os

@cl.on_chat_start
async def start():
    await cl.Message(
        content="Upload a file to test. Drag and drop or use the attachment button."
    ).send()

@cl.on_message
async def main(message: cl.Message):
    # Check for file uploads
    if not message.elements:
        await cl.Message(content="No files attached. Please upload a file.").send()
        return
    
    # Process each element
    for element in message.elements:
        if isinstance(element, cl.File):
            # Debug information
            msg = f"""📁 **File Upload Debug Info**
            
**Name**: {element.name}
**Type**: {type(element)}
**Has path**: {hasattr(element, 'path')}
**Path**: {getattr(element, 'path', 'N/A')}
**Has content**: {hasattr(element, 'content')}
**Content type**: {type(getattr(element, 'content', None))}
**Size**: {element.size if hasattr(element, 'size') else 'Unknown'}
**MIME**: {element.mime if hasattr(element, 'mime') else 'Unknown'}

**All attributes**: {[attr for attr in dir(element) if not attr.startswith('_')]}
            """
            
            await cl.Message(content=msg).send()
            
            # Try to read the file
            try:
                # Method 1: Direct path
                if hasattr(element, 'path') and element.path and os.path.exists(element.path):
                    with open(element.path, 'r') as f:
                        content = f.read()[:100]  # First 100 chars
                    await cl.Message(content=f"✅ Read from path. First 100 chars:\n```\n{content}\n```").send()
                    
                # Method 2: Read method
                elif hasattr(element, 'read'):
                    content = await element.read()
                    if content:
                        text = content.decode('utf-8')[:100] if isinstance(content, bytes) else str(content)[:100]
                        await cl.Message(content=f"✅ Read using read(). First 100 chars:\n```\n{text}\n```").send()
                    
                else:
                    await cl.Message(content="❌ Could not find a way to read the file content.").send()
                    
            except Exception as e:
                await cl.Message(content=f"❌ Error reading file: {str(e)}").send()

if __name__ == "__main__":
    cl.run() 