## Checkpoint-Based Recovery System

### The Problem

Processing large datasets (millions of rows) in a single transaction creates two critical risks:

1. **Memory Exhaustion**: Loading 2.5M rows into memory could slow down the worker
2. **Catastrophic Failure**: A failure at row 1,999,999 forces reprocessing of all 2.5M rows

**Alternative considered**: Full intermediate Silver layer was rejected to avoid additional cost.

### The Solution: Batched Transform-Load with XCom Checkpointing

The system processes data in 100k-row batches, saving progress after each successful batch to Airflow Variables. On failure, the pipeline resumes from the last checkpoint rather than starting over.


The checkpoint system consists of three components working together:

#### 1. **LoadState** (Data Class)
```python
@dataclass
class LoadState:
    last_batch_number: int  # Last successfully completed batch
    rows_loaded: int        # Total rows loaded so far
```

Type-safe representation of checkpoint state. Provides clear contract for what gets persisted.

#### 2. **StateLoader** (Stateless Utility)
```python
class StateLoader:
    @staticmethod
    def get_state(context: dict) -> LoadState:
        """Retrieves checkpoint from Airflow Variable on task retry."""
        task_id = context['task_instance'].task_id
        dag_id = context['dag'].dag_id
        execution_date = context['logical_date'].format('YYYY-MM-DD')
        
        checkpoint_key = f"{dag_id}_{task_id}_{execution_date}"
        
        # Fetch from Airflow Variable
        state_json = Variable.get(checkpoint_key, default_var=None)
        
        if state_json:
            return LoadState(**json.loads(state_json))
        return LoadState(last_batch_number=0, rows_loaded=0)
```

**Why stateless?** Separation of concerns. StateLoader retrieves state; Loader manages loading. Makes testing easier.

#### 3. **Loader** (Orchestrator)
```python
class Loader:
    def __init__(self, context: dict):
        self.context = context
        self.state = StateLoader.get_state(context)  # Load checkpoint
        self.connection = self._create_connection()
    
    def load_batches(self, df: DataFrame, batch_size: int = 100_000):
        """Load data in batches with checkpoint persistence."""
        
        # Resume from last checkpoint
        start_batch = self.state.last_batch_number
        
        for batch_num in range(start_batch, len(df) // batch_size + 1):
            batch_df = df[batch_num * batch_size : (batch_num + 1) * batch_size]
            
            # Load batch to database
            self.load_to_database(batch_df)
            
            # Update state
            self.state.last_batch_number = batch_num + 1
            self.state.rows_loaded += len(batch_df)
            
            # Persist checkpoint
            self.save_checkpoint()
    
    def save_checkpoint(self):
        """Persist current state to Airflow Variable."""
        task_id = self.context['task_instance'].task_id
        dag_id = self.context['dag'].dag_id
        execution_date = self.context['logical_date'].format('YYYY-MM-DD')
        
        checkpoint_key = f"{dag_id}_{task_id}_{execution_date}"
        
        Variable.set(
            checkpoint_key,
            json.dumps(asdict(self.state))
        )
```

---

### Execution Flow

#### **First Attempt (Success)**
```
Batch 1: Load 100k rows → save_checkpoint(batch=1, rows=100k)
Batch 2: Load 100k rows → save_checkpoint(batch=2, rows=200k)
Batch 3: Load 100k rows → save_checkpoint(batch=3, rows=300k)
Task succeeds
```

#### **Second Attempt (Failure + Retry)**
```
Batch 1: Load 100k rows → save_checkpoint(batch=1, rows=100k)
Batch 2: Load 100k rows → save_checkpoint(batch=2, rows=200k)
Batch 3: DATABASE CONNECTION TIMEOUT
Task fails, Airflow triggers retry

--- RETRY BEGINS ---
StateLoader.get_state() → LoadState(last_batch_number=2, rows_loaded=200k)
Resume from Batch 3 (skip already-loaded batches)
Batch 3: Load 100k rows → save_checkpoint(batch=3, rows=300k)
Task succeeds
```

**Key Insight**: Only Batch 3 (100k rows) is reprocessed. Batches 1-2 (200k rows) are skipped because the checkpoint indicates they already succeeded.

---

### Why This Works

**Idempotent Batch Loading**: Each batch writes to the same `staging` schema with `TRUNCATE` before load, then `MERGE` into final table. Re-running Batch 3 produces identical results.

**Airflow Variable as State Store**:
- Persistent across task retries
- Keyed by `dag_id + task_id + execution_date` for uniqueness
- Negligible cost (~$0.001 per checkpoint)

**Batch Size Optimization**: 100k rows balances:
- Memory efficiency (avoids OOM)
- Network overhead (fewer round-trips than 10k batches)
- Retry granularity (max 100k rows reprocessed on failure)

---

### Monitoring & Observability

**Checkpoint Metrics**:

BEFORE LOAD
```python
log.info(
  f"{total_rows} {entity_type} will be loaded in " f"{batches_to_run} batches"
        )
```
AFTER EACH BATCH

```python
log.info(f"Checkpoint saved: batch={self.state.last_batch_number}, "
         f"rows_loaded={self.state.rows_loaded}, "
         f"progress={self.state.rows_loaded / total_rows * 100:.1f}%")
```
```python
log.info(
  f"{total_rows} {entity_type} will be loaded in " f"{batches_to_run} batches"
        )
```


**Cleanup Logic** (post-success):
```python
def cleanup_checkpoint(self):
    """Remove checkpoint after successful completion."""
    checkpoint_key = f"{self.dag_id}_{self.task_id}_{self.execution_date}"
    Variable.delete(checkpoint_key)
```

Cleanup prevents MetaDatabase bloat. Checkpoints are only needed during active processing, and can be discarded after to free up space

