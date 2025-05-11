from pydantic import BaseModel, Field


class Config(BaseModel):
    """
    Configuration class for the reddit config.
    """

    REDDIT_CLIENT_ID: str = Field(..., env="REDDIT_CLIENT_ID")
    REDDIT_CLIENT_SECRET: str = Field(..., env="REDDIT_CLIENT_SECRET")
    REDDIT_USER_AGENT: str = Field(..., env="REDDIT_USER_AGENT")
    REDDIT_RATE_LIMIT: int = Field(60, env="REDDIT_RATE_LIMIT")  # Default to 60 seconds
