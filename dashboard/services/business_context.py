"""
Business Context Service - DynamoDB storage for employee-provided business insights.

This module provides storage and retrieval of business context that employees
can provide to help the insights engine generate more relevant recommendations.
Context can include information like:
- Brand/product discontinuation notices
- Operational changes
- Strategic direction notes
- Market insights
"""

import uuid
from datetime import datetime
from typing import Dict, List, Optional
from decimal import Decimal

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class BusinessContextService:
    """
    Service for storing and retrieving business context from DynamoDB.
    Context entries are used by the insights engine to generate more relevant insights.
    """

    TABLE_NAME = 'retail-business-context'

    def __init__(self, aws_access_key: str = None, aws_secret_key: str = None, region: str = 'us-west-1'):
        """Initialize DynamoDB client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 required. Install with: pip install boto3")

        # Configure timeouts to prevent hanging on network issues
        from botocore.config import Config
        boto_config = Config(
            connect_timeout=5,
            read_timeout=10,
            retries={'max_attempts': 2}
        )

        session_kwargs = {'region_name': region, 'config': boto_config}
        if aws_access_key and aws_secret_key:
            session_kwargs['aws_access_key_id'] = aws_access_key
            session_kwargs['aws_secret_access_key'] = aws_secret_key

        self.dynamodb = boto3.resource('dynamodb', **session_kwargs)
        self.region = region
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create the business context table if it doesn't exist."""
        try:
            table = self.dynamodb.create_table(
                TableName=self.TABLE_NAME,
                KeySchema=[
                    {'AttributeName': 'context_id', 'KeyType': 'HASH'},
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'context_id', 'AttributeType': 'S'},
                    {'AttributeName': 'created_date', 'AttributeType': 'S'},
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'date-index',
                        'KeySchema': [
                            {'AttributeName': 'created_date', 'KeyType': 'HASH'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            # Table already exists
            pass
        except Exception as e:
            # Log but don't fail - table might exist
            print(f"Note: Could not create/verify table: {e}")

    def add_context(self, context_text: str, author_name: str) -> Dict:
        """
        Add a new business context entry.

        Args:
            context_text: The business context/insight text
            author_name: Name of the person providing the context

        Returns:
            Dictionary with the created context entry details
        """
        table = self.dynamodb.Table(self.TABLE_NAME)

        context_id = str(uuid.uuid4())
        created_at = datetime.now()
        created_date = created_at.strftime('%Y-%m-%d')

        item = {
            'context_id': context_id,
            'context_text': context_text,
            'author_name': author_name,
            'created_at': created_at.isoformat(),
            'created_date': created_date,
            'is_active': True
        }

        table.put_item(Item=item)

        return {
            'context_id': context_id,
            'created_at': created_at.isoformat(),
            'created_date': created_date,
            'author_name': author_name,
            'context_text': context_text
        }

    def get_all_context(self, active_only: bool = True) -> List[Dict]:
        """
        Retrieve all business context entries.

        Args:
            active_only: If True, only return active (non-deleted) entries

        Returns:
            List of context entries sorted by date (newest first)
        """
        table = self.dynamodb.Table(self.TABLE_NAME)

        try:
            response = table.scan()
            items = response.get('Items', [])

            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))

            # Filter active only if requested
            if active_only:
                items = [item for item in items if item.get('is_active', True)]

            # Sort by created_at descending (newest first)
            items.sort(key=lambda x: x.get('created_at', ''), reverse=True)

            return items

        except ClientError as e:
            print(f"Error fetching context: {e}")
            return []

    def get_context_for_insights(self) -> List[Dict]:
        """
        Get context entries formatted for the insights engine.

        Returns:
            List of active context entries with relevant fields
        """
        items = self.get_all_context(active_only=True)

        return [
            {
                'id': item.get('context_id'),
                'text': item.get('context_text'),
                'author': item.get('author_name'),
                'date': item.get('created_date'),
                'created_at': item.get('created_at')
            }
            for item in items
        ]

    def deactivate_context(self, context_id: str) -> bool:
        """
        Soft-delete a context entry by marking it inactive.

        Args:
            context_id: The ID of the context entry to deactivate

        Returns:
            True if successful, False otherwise
        """
        table = self.dynamodb.Table(self.TABLE_NAME)

        try:
            table.update_item(
                Key={'context_id': context_id},
                UpdateExpression='SET is_active = :val',
                ExpressionAttributeValues={':val': False}
            )
            return True
        except ClientError as e:
            print(f"Error deactivating context {context_id}: {e}")
            return False

    def delete_context(self, context_id: str) -> bool:
        """
        Permanently delete a context entry.

        Args:
            context_id: The ID of the context entry to delete

        Returns:
            True if successful, False otherwise
        """
        table = self.dynamodb.Table(self.TABLE_NAME)

        try:
            table.delete_item(Key={'context_id': context_id})
            return True
        except ClientError as e:
            print(f"Error deleting context {context_id}: {e}")
            return False

    def get_context_count(self) -> int:
        """Get the count of active business context entries."""
        items = self.get_all_context(active_only=True)
        return len(items)


def get_business_context_service(aws_config: Dict = None) -> Optional[BusinessContextService]:
    """
    Factory function to create a BusinessContextService from config.

    Args:
        aws_config: Dictionary with aws_access_key, aws_secret_key, region

    Returns:
        BusinessContextService instance or None if initialization fails
    """
    if not BOTO3_AVAILABLE:
        return None

    try:
        # Try Streamlit secrets first
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'aws' in st.secrets:
                return BusinessContextService(
                    aws_access_key=st.secrets['aws'].get('access_key_id'),
                    aws_secret_key=st.secrets['aws'].get('secret_access_key'),
                    region=st.secrets['aws'].get('region', 'us-west-1')
                )
        except Exception:
            pass

        # Fall back to provided config
        if aws_config:
            return BusinessContextService(
                aws_access_key=aws_config.get('aws_access_key'),
                aws_secret_key=aws_config.get('aws_secret_key'),
                region=aws_config.get('region', 'us-west-1')
            )

        # Try environment variables
        import os
        if os.environ.get('AWS_ACCESS_KEY_ID'):
            return BusinessContextService(
                aws_access_key=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                region=os.environ.get('AWS_REGION', 'us-west-1')
            )

        return None

    except Exception as e:
        print(f"Failed to initialize BusinessContextService: {e}")
        return None
