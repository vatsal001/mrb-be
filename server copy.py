# from fastapi import FastAPI, APIRouter, HTTPException, status, Depends, UploadFile, File, Response
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from dotenv import load_dotenv
# from starlette.middleware.cors import CORSMiddleware
# from motor.motor_asyncio import AsyncIOMotorClient
# import os
# import logging
# from pathlib import Path
# from pydantic import BaseModel, Field, ConfigDict
# from typing import List, Optional
# import uuid
# from datetime import datetime, timezone, timedelta
# import bcrypt
# import jwt
# import barcode
# from barcode.writer import ImageWriter
# from io import BytesIO
# import base64
# from openpyxl import Workbook
# from openpyxl.styles import Font, Alignment

# ROOT_DIR = Path(__file__).parent
# load_dotenv(ROOT_DIR / '.env')

# mongo_url = os.environ['MONGO_URL']
# client = AsyncIOMotorClient(mongo_url)
# db = client[os.environ['DB_NAME']]

# app = FastAPI()
# api_router = APIRouter(prefix="/api")
# security = HTTPBearer()

# JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-in-production')


from fastapi import FastAPI, APIRouter, HTTPException, Depends, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from contextlib import asynccontextmanager
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
import barcode
from barcode.writer import ImageWriter
from io import BytesIO
import base64
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

api_router = APIRouter(prefix="/api")
security = HTTPBearer()

JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-in-production')

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown
    client.close()

app = FastAPI(lifespan=lifespan)
app.include_router(api_router)

class User(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    email: str
    name: str
    role: str = "staff"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class UserCreate(BaseModel):
    email: str
    password: str
    name: str
    role: Optional[str] = "staff"

class UserLogin(BaseModel):
    email: str
    password: str

class Product(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    sku: str
    barcode: str
    category: str
    purchase_price: float
    selling_price: float
    stock_quantity: int
    supplier: Optional[str] = ""
    image_url: Optional[str] = ""
    low_stock_threshold: int = 10
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ProductCreate(BaseModel):
    name: str
    sku: str
    category: str
    purchase_price: float
    selling_price: float
    stock_quantity: int
    supplier: Optional[str] = ""
    image_url: Optional[str] = ""
    low_stock_threshold: Optional[int] = 10

class ProductUpdate(BaseModel):
    name: Optional[str] = None
    sku: Optional[str] = None
    category: Optional[str] = None
    purchase_price: Optional[float] = None
    selling_price: Optional[float] = None
    stock_quantity: Optional[int] = None
    supplier: Optional[str] = None
    image_url: Optional[str] = None
    low_stock_threshold: Optional[int] = None

class OrderItem(BaseModel):
    product_id: str
    product_name: str
    quantity: int
    price: float
    total: float

class Order(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    invoice_number: str
    items: List[OrderItem]
    subtotal: float
    tax: float
    discount: float
    total: float
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class OrderCreate(BaseModel):
    items: List[OrderItem]
    subtotal: float
    tax: float
    discount: float
    total: float

# Rack & Location Management Models
class Location(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    type: str  # 'mall' or 'warehouse'
    description: Optional[str] = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class LocationCreate(BaseModel):
    name: str
    type: str
    description: Optional[str] = ""

class Rack(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    code: str
    name: str
    location_id: str
    location_name: str
    description: Optional[str] = ""
    max_capacity: Optional[int] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class RackCreate(BaseModel):
    code: str
    name: str
    location_id: str
    description: Optional[str] = ""
    max_capacity: Optional[int] = None

class RackUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    location_id: Optional[str] = None
    description: Optional[str] = None
    max_capacity: Optional[int] = None

class RackAssignment(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    product_id: str
    product_name: str
    rack_id: str
    rack_code: str
    location_id: str
    location_name: str
    quantity: int
    assigned_by: str
    assigned_by_name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class RackAssignmentCreate(BaseModel):
    product_id: str
    rack_id: str
    quantity: int

class RackAssignmentUpdate(BaseModel):
    quantity: int

class StockTransfer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    product_id: str
    product_name: str
    from_rack_id: Optional[str] = None
    from_rack_code: Optional[str] = None
    to_rack_id: Optional[str] = None
    to_rack_code: Optional[str] = None
    quantity: int
    transfer_type: str  # 'manual', 'sale', 'restock'
    notes: Optional[str] = ""
    transferred_by: str
    transferred_by_name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class StockTransferCreate(BaseModel):
    product_id: str
    from_rack_id: Optional[str] = None
    to_rack_id: Optional[str] = None
    quantity: int
    transfer_type: str = 'manual'
    notes: Optional[str] = ""

class ProductLocationThreshold(BaseModel):
    product_id: str
    mall_threshold: int = 5
    warehouse_threshold: int = 20

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_token(user_id: str, email: str, role: str) -> str:
    payload = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'exp': datetime.now(timezone.utc) + timedelta(days=7)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user = await db.users.find_one({'id': payload['user_id']}, {'_id': 0})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return User(**user)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

def generate_barcode_image(code: str) -> str:
    EAN = barcode.get_barcode_class('code128')
    ean = EAN(code, writer=ImageWriter())
    buffer = BytesIO()
    ean.write(buffer)
    buffer.seek(0)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')

@api_router.post("/auth/register", response_model=User)
async def register(user_data: UserCreate):
    existing = await db.users.find_one({'email': user_data.email}, {'_id': 0})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user = User(
        email=user_data.email,
        name=user_data.name,
        role=user_data.role
    )
    doc = user.model_dump()
    doc['password'] = hash_password(user_data.password)
    doc['created_at'] = doc['created_at'].isoformat()
    
    await db.users.insert_one(doc)
    return user

@api_router.post("/auth/login")
async def login(credentials: UserLogin):
    user_doc = await db.users.find_one({'email': credentials.email}, {'_id': 0})
    if not user_doc or not verify_password(credentials.password, user_doc['password']):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_token(user_doc['id'], user_doc['email'], user_doc['role'])
    return {
        'token': token,
        'user': {
            'id': user_doc['id'],
            'email': user_doc['email'],
            'name': user_doc['name'],
            'role': user_doc['role']
        }
    }

@api_router.get("/auth/me", response_model=User)
async def get_me(current_user: User = Depends(get_current_user)):
    return current_user

@api_router.get("/products", response_model=List[Product])
async def get_products(current_user: User = Depends(get_current_user)):
    products = await db.products.find({}, {'_id': 0}).to_list(1000)
    for product in products:
        if isinstance(product.get('created_at'), str):
            product['created_at'] = datetime.fromisoformat(product['created_at'])
    return products

@api_router.post("/products", response_model=Product)
async def create_product(product_data: ProductCreate, current_user: User = Depends(get_current_user)):
    if current_user.role not in ['admin', 'staff']:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    barcode_num = str(uuid.uuid4().int)[:12]
    product = Product(
        **product_data.model_dump(),
        barcode=barcode_num
    )
    doc = product.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    
    await db.products.insert_one(doc)
    return product

@api_router.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: str, current_user: User = Depends(get_current_user)):
    product = await db.products.find_one({'id': product_id}, {'_id': 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    if isinstance(product.get('created_at'), str):
        product['created_at'] = datetime.fromisoformat(product['created_at'])
    return Product(**product)

@api_router.get("/products/barcode/{barcode_num}")
async def get_product_by_barcode(barcode_num: str, current_user: User = Depends(get_current_user)):
    product = await db.products.find_one({'barcode': barcode_num}, {'_id': 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    if isinstance(product.get('created_at'), str):
        product['created_at'] = datetime.fromisoformat(product['created_at'])
    return product

@api_router.put("/products/{product_id}", response_model=Product)
async def update_product(product_id: str, update_data: ProductUpdate, current_user: User = Depends(get_current_user)):
    if current_user.role not in ['admin', 'staff']:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    existing = await db.products.find_one({'id': product_id}, {'_id': 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Product not found")
    
    update_dict = {k: v for k, v in update_data.model_dump().items() if v is not None}
    if update_dict:
        await db.products.update_one({'id': product_id}, {'$set': update_dict})
    
    updated = await db.products.find_one({'id': product_id}, {'_id': 0})
    if isinstance(updated.get('created_at'), str):
        updated['created_at'] = datetime.fromisoformat(updated['created_at'])
    return Product(**updated)

@api_router.delete("/products/{product_id}")
async def delete_product(product_id: str, current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can delete products")
    
    result = await db.products.delete_one({'id': product_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {'message': 'Product deleted successfully'}

@api_router.get("/products/{product_id}/barcode-image")
async def get_barcode_image(product_id: str, current_user: User = Depends(get_current_user)):
    product = await db.products.find_one({'id': product_id}, {'_id': 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    barcode_img = generate_barcode_image(product['barcode'])
    return {'barcode_image': f'data:image/png;base64,{barcode_img}'}

@api_router.post("/orders", response_model=Order)
async def create_order(order_data: OrderCreate, current_user: User = Depends(get_current_user)):
    invoice_num = f"INV-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8].upper()}"
    
    order = Order(
        invoice_number=invoice_num,
        items=order_data.items,
        subtotal=order_data.subtotal,
        tax=order_data.tax,
        discount=order_data.discount,
        total=order_data.total,
        created_by=current_user.id
    )
    
    # Get mall locations
    mall_locations = await db.locations.find({'type': 'mall'}, {'_id': 0}).to_list(10)
    mall_location_ids = [loc['id'] for loc in mall_locations]
    
    for item in order_data.items:
        product = await db.products.find_one({'id': item.product_id}, {'_id': 0})
        if product:
            # Update main inventory
            new_stock = product['stock_quantity'] - item.quantity
            await db.products.update_one(
                {'id': item.product_id},
                {'$set': {'stock_quantity': max(0, new_stock)}}
            )
            
            # Deduct from mall racks (FIFO - highest quantity first)
            remaining_qty = item.quantity
            mall_assignments = await db.rack_assignments.find({
                'product_id': item.product_id,
                'location_id': {'$in': mall_location_ids}
            }, {'_id': 0}).sort('quantity', -1).to_list(100)
            
            for assignment in mall_assignments:
                if remaining_qty <= 0:
                    break
                
                deduct_qty = min(remaining_qty, assignment['quantity'])
                new_rack_qty = assignment['quantity'] - deduct_qty
                
                if new_rack_qty == 0:
                    await db.rack_assignments.delete_one({'id': assignment['id']})
                else:
                    await db.rack_assignments.update_one(
                        {'id': assignment['id']},
                        {'$set': {
                            'quantity': new_rack_qty,
                            'updated_at': datetime.now(timezone.utc).isoformat()
                        }}
                    )
                
                # Log the sale deduction
                transfer = StockTransfer(
                    product_id=item.product_id,
                    product_name=product['name'],
                    from_rack_id=assignment['rack_id'],
                    from_rack_code=assignment['rack_code'],
                    to_rack_id=None,
                    to_rack_code=None,
                    quantity=deduct_qty,
                    transfer_type='sale',
                    notes=f"Sale deduction from invoice {invoice_num}",
                    transferred_by=current_user.id,
                    transferred_by_name=current_user.name
                )
                
                transfer_doc = transfer.model_dump()
                transfer_doc['created_at'] = transfer_doc['created_at'].isoformat()
                await db.stock_transfers.insert_one(transfer_doc)
                
                remaining_qty -= deduct_qty
    
    doc = order.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.orders.insert_one(doc)
    
    return order

@api_router.get("/orders", response_model=List[Order])
async def get_orders(current_user: User = Depends(get_current_user)):
    orders = await db.orders.find({}, {'_id': 0}).sort('created_at', -1).to_list(1000)
    for order in orders:
        if isinstance(order.get('created_at'), str):
            order['created_at'] = datetime.fromisoformat(order['created_at'])
    return orders

@api_router.get("/orders/{order_id}", response_model=Order)
async def get_order(order_id: str, current_user: User = Depends(get_current_user)):
    order = await db.orders.find_one({'id': order_id}, {'_id': 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if isinstance(order.get('created_at'), str):
        order['created_at'] = datetime.fromisoformat(order['created_at'])
    return Order(**order)

@api_router.get("/reports/sales")
async def get_sales_report(period: str = "daily", current_user: User = Depends(get_current_user)):
    now = datetime.now(timezone.utc)
    
    if period == "daily":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "weekly":
        start_date = now - timedelta(days=7)
    elif period == "monthly":
        start_date = now - timedelta(days=30)
    else:
        start_date = now - timedelta(days=30)
    
    orders = await db.orders.find({}, {'_id': 0}).to_list(10000)
    
    filtered_orders = []
    for order in orders:
        created_at = order.get('created_at')
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if created_at >= start_date:
            filtered_orders.append(order)
    
    total_sales = sum(order['total'] for order in filtered_orders)
    total_orders = len(filtered_orders)
    
    products = await db.products.find({}, {'_id': 0}).to_list(1000)
    total_profit = 0
    for order in filtered_orders:
        for item in order['items']:
            product = next((p for p in products if p['id'] == item['product_id']), None)
            if product:
                profit = (item['price'] - product['purchase_price']) * item['quantity']
                total_profit += profit
    
    return {
        'period': period,
        'total_sales': round(total_sales, 2),
        'total_orders': total_orders,
        'total_profit': round(total_profit, 2),
        'orders': filtered_orders
    }

@api_router.get("/reports/export/excel")
async def export_excel_report(period: str = "daily", current_user: User = Depends(get_current_user)):
    report_data = await get_sales_report(period, current_user)
    
    wb = Workbook()
    ws = wb.active
    ws.title = f"{period.capitalize()} Sales Report"
    
    ws['A1'] = f"Sales Report - {period.capitalize()}"
    ws['A1'].font = Font(size=14, bold=True)
    
    ws['A3'] = 'Total Sales:'
    ws['B3'] = report_data['total_sales']
    ws['A4'] = 'Total Orders:'
    ws['B4'] = report_data['total_orders']
    ws['A5'] = 'Total Profit:'
    ws['B5'] = report_data['total_profit']
    
    headers = ['Invoice Number', 'Date', 'Items', 'Subtotal', 'Tax', 'Discount', 'Total']
    for col, header in enumerate(headers, start=1):
        cell = ws.cell(row=7, column=col, value=header)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center')
    
    for row_idx, order in enumerate(report_data['orders'], start=8):
        ws.cell(row=row_idx, column=1, value=order['invoice_number'])
        created_at = order['created_at']
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        ws.cell(row=row_idx, column=2, value=created_at.strftime('%Y-%m-%d %H:%M'))
        ws.cell(row=row_idx, column=3, value=len(order['items']))
        ws.cell(row=row_idx, column=4, value=order['subtotal'])
        ws.cell(row=row_idx, column=5, value=order['tax'])
        ws.cell(row=row_idx, column=6, value=order['discount'])
        ws.cell(row=row_idx, column=7, value=order['total'])
    
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    
    return Response(
        content=buffer.getvalue(),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename=sales_report_{period}.xlsx'}
    )

@api_router.get("/dashboard/stats")
async def get_dashboard_stats(current_user: User = Depends(get_current_user)):
    products = await db.products.find({}, {'_id': 0}).to_list(1000)
    orders = await db.orders.find({}, {'_id': 0}).to_list(1000)
    
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    today_orders = []
    for order in orders:
        created_at = order.get('created_at')
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if created_at >= today_start:
            today_orders.append(order)
    
    today_sales = sum(order['total'] for order in today_orders)
    total_sales = sum(order['total'] for order in orders)
    
    total_profit = 0
    for order in orders:
        for item in order['items']:
            product = next((p for p in products if p['id'] == item['product_id']), None)
            if product:
                profit = (item['price'] - product['purchase_price']) * item['quantity']
                total_profit += profit
    
    low_stock_products = [p for p in products if p['stock_quantity'] <= p.get('low_stock_threshold', 10)]
    
    return {
        'today_sales': round(today_sales, 2),
        'total_sales': round(total_sales, 2),
        'total_profit': round(total_profit, 2),
        'total_products': len(products),
        'total_orders': len(orders),
        'low_stock_count': len(low_stock_products),
        'low_stock_products': low_stock_products[:5]
    }

@api_router.get("/users", response_model=List[User])
async def get_users(current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can view users")
    users = await db.users.find({}, {'_id': 0, 'password': 0}).to_list(1000)
    for user in users:
        if isinstance(user.get('created_at'), str):
            user['created_at'] = datetime.fromisoformat(user['created_at'])
    return users

@api_router.put("/users/{user_id}/role")
async def update_user_role(user_id: str, role: str, current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can update roles")
    
    result = await db.users.update_one({'id': user_id}, {'$set': {'role': role}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {'message': 'Role updated successfully'}

# ==================== RACK & LOCATION MANAGEMENT ENDPOINTS ====================

# Locations
@api_router.get("/locations", response_model=List[Location])
async def get_locations(current_user: User = Depends(get_current_user)):
    locations = await db.locations.find({}, {'_id': 0}).to_list(1000)
    for loc in locations:
        if isinstance(loc.get('created_at'), str):
            loc['created_at'] = datetime.fromisoformat(loc['created_at'])
    return locations

@api_router.post("/locations", response_model=Location)
async def create_location(location_data: LocationCreate, current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can create locations")
    
    location = Location(**location_data.model_dump())
    doc = location.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.locations.insert_one(doc)
    return location

# Racks
@api_router.get("/racks", response_model=List[Rack])
async def get_racks(location_id: Optional[str] = None, current_user: User = Depends(get_current_user)):
    query = {'location_id': location_id} if location_id else {}
    racks = await db.racks.find(query, {'_id': 0}).to_list(1000)
    for rack in racks:
        if isinstance(rack.get('created_at'), str):
            rack['created_at'] = datetime.fromisoformat(rack['created_at'])
    return racks

@api_router.post("/racks", response_model=Rack)
async def create_rack(rack_data: RackCreate, current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can create racks")
    
    # Get location name
    location = await db.locations.find_one({'id': rack_data.location_id}, {'_id': 0})
    if not location:
        raise HTTPException(status_code=404, detail="Location not found")
    
    # Check for duplicate rack code in same location
    existing = await db.racks.find_one({
        'code': rack_data.code,
        'location_id': rack_data.location_id
    }, {'_id': 0})
    if existing:
        raise HTTPException(status_code=400, detail="Rack code already exists in this location")
    
    rack = Rack(
        **rack_data.model_dump(),
        location_name=location['name']
    )
    doc = rack.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.racks.insert_one(doc)
    return rack

@api_router.get("/racks/{rack_id}", response_model=Rack)
async def get_rack(rack_id: str, current_user: User = Depends(get_current_user)):
    rack = await db.racks.find_one({'id': rack_id}, {'_id': 0})
    if not rack:
        raise HTTPException(status_code=404, detail="Rack not found")
    if isinstance(rack.get('created_at'), str):
        rack['created_at'] = datetime.fromisoformat(rack['created_at'])
    return Rack(**rack)

@api_router.put("/racks/{rack_id}", response_model=Rack)
async def update_rack(rack_id: str, update_data: RackUpdate, current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can update racks")
    
    existing = await db.racks.find_one({'id': rack_id}, {'_id': 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Rack not found")
    
    update_dict = {k: v for k, v in update_data.model_dump().items() if v is not None}
    
    if 'location_id' in update_dict:
        location = await db.locations.find_one({'id': update_dict['location_id']}, {'_id': 0})
        if location:
            update_dict['location_name'] = location['name']
    
    if update_dict:
        await db.racks.update_one({'id': rack_id}, {'$set': update_dict})
    
    updated = await db.racks.find_one({'id': rack_id}, {'_id': 0})
    if isinstance(updated.get('created_at'), str):
        updated['created_at'] = datetime.fromisoformat(updated['created_at'])
    return Rack(**updated)

@api_router.delete("/racks/{rack_id}")
async def delete_rack(rack_id: str, current_user: User = Depends(get_current_user)):
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can delete racks")
    
    # Check if rack has assignments
    assignments = await db.rack_assignments.find_one({'rack_id': rack_id}, {'_id': 0})
    if assignments:
        raise HTTPException(status_code=400, detail="Cannot delete rack with product assignments")
    
    result = await db.racks.delete_one({'id': rack_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Rack not found")
    return {'message': 'Rack deleted successfully'}

# Rack Assignments
@api_router.get("/products/{product_id}/rack-assignments", response_model=List[RackAssignment])
async def get_product_rack_assignments(product_id: str, current_user: User = Depends(get_current_user)):
    assignments = await db.rack_assignments.find({'product_id': product_id}, {'_id': 0}).to_list(1000)
    for assignment in assignments:
        if isinstance(assignment.get('created_at'), str):
            assignment['created_at'] = datetime.fromisoformat(assignment['created_at'])
        if isinstance(assignment.get('updated_at'), str):
            assignment['updated_at'] = datetime.fromisoformat(assignment['updated_at'])
    return assignments

@api_router.get("/racks/{rack_id}/products")
async def get_rack_products(rack_id: str, current_user: User = Depends(get_current_user)):
    assignments = await db.rack_assignments.find({'rack_id': rack_id}, {'_id': 0}).to_list(1000)
    return assignments

@api_router.post("/rack-assignments", response_model=RackAssignment)
async def create_rack_assignment(assignment_data: RackAssignmentCreate, current_user: User = Depends(get_current_user)):
    # Get product details
    product = await db.products.find_one({'id': assignment_data.product_id}, {'_id': 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Get rack details
    rack = await db.racks.find_one({'id': assignment_data.rack_id}, {'_id': 0})
    if not rack:
        raise HTTPException(status_code=404, detail="Rack not found")
    
    # Check if assignment already exists
    existing = await db.rack_assignments.find_one({
        'product_id': assignment_data.product_id,
        'rack_id': assignment_data.rack_id
    }, {'_id': 0})
    
    if existing:
        raise HTTPException(status_code=400, detail="Product already assigned to this rack")
    
    # Check total assigned quantity doesn't exceed stock
    assignments = await db.rack_assignments.find({'product_id': assignment_data.product_id}, {'_id': 0}).to_list(1000)
    total_assigned = sum(a['quantity'] for a in assignments) + assignment_data.quantity
    
    if total_assigned > product['stock_quantity']:
        raise HTTPException(
            status_code=400,
            detail=f"Total assigned quantity ({total_assigned}) exceeds available stock ({product['stock_quantity']})"
        )
    
    assignment = RackAssignment(
        product_id=assignment_data.product_id,
        product_name=product['name'],
        rack_id=assignment_data.rack_id,
        rack_code=rack['code'],
        location_id=rack['location_id'],
        location_name=rack['location_name'],
        quantity=assignment_data.quantity,
        assigned_by=current_user.id,
        assigned_by_name=current_user.name
    )
    
    doc = assignment.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['updated_at'] = doc['updated_at'].isoformat()
    await db.rack_assignments.insert_one(doc)
    return assignment

@api_router.put("/rack-assignments/{assignment_id}", response_model=RackAssignment)
async def update_rack_assignment(
    assignment_id: str,
    update_data: RackAssignmentUpdate,
    current_user: User = Depends(get_current_user)
):
    existing = await db.rack_assignments.find_one({'id': assignment_id}, {'_id': 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Assignment not found")
    
    # Check total assigned quantity
    product = await db.products.find_one({'id': existing['product_id']}, {'_id': 0})
    assignments = await db.rack_assignments.find({
        'product_id': existing['product_id'],
        'id': {'$ne': assignment_id}
    }, {'_id': 0}).to_list(1000)
    
    total_assigned = sum(a['quantity'] for a in assignments) + update_data.quantity
    
    if total_assigned > product['stock_quantity']:
        raise HTTPException(
            status_code=400,
            detail=f"Total assigned quantity ({total_assigned}) exceeds available stock ({product['stock_quantity']})"
        )
    
    await db.rack_assignments.update_one(
        {'id': assignment_id},
        {'$set': {
            'quantity': update_data.quantity,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }}
    )
    
    updated = await db.rack_assignments.find_one({'id': assignment_id}, {'_id': 0})
    if isinstance(updated.get('created_at'), str):
        updated['created_at'] = datetime.fromisoformat(updated['created_at'])
    if isinstance(updated.get('updated_at'), str):
        updated['updated_at'] = datetime.fromisoformat(updated['updated_at'])
    return RackAssignment(**updated)

@api_router.delete("/rack-assignments/{assignment_id}")
async def delete_rack_assignment(assignment_id: str, current_user: User = Depends(get_current_user)):
    result = await db.rack_assignments.delete_one({'id': assignment_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Assignment not found")
    return {'message': 'Assignment deleted successfully'}

# Stock Transfers
@api_router.get("/stock-transfers", response_model=List[StockTransfer])
async def get_stock_transfers(
    product_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    query = {}
    if product_id:
        query['product_id'] = product_id
    
    transfers = await db.stock_transfers.find(query, {'_id': 0}).sort('created_at', -1).to_list(1000)
    
    # Filter by date if provided
    if start_date or end_date:
        filtered = []
        for transfer in transfers:
            created_at = transfer.get('created_at')
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at)
            
            if start_date and created_at < datetime.fromisoformat(start_date):
                continue
            if end_date and created_at > datetime.fromisoformat(end_date):
                continue
            filtered.append(transfer)
        transfers = filtered
    
    for transfer in transfers:
        if isinstance(transfer.get('created_at'), str):
            transfer['created_at'] = datetime.fromisoformat(transfer['created_at'])
    
    return transfers

@api_router.post("/stock-transfers", response_model=StockTransfer)
async def create_stock_transfer(transfer_data: StockTransferCreate, current_user: User = Depends(get_current_user)):
    # Get product
    product = await db.products.find_one({'id': transfer_data.product_id}, {'_id': 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    from_rack_code = None
    to_rack_code = None
    
    # Validate and update source rack
    if transfer_data.from_rack_id:
        from_assignment = await db.rack_assignments.find_one({
            'product_id': transfer_data.product_id,
            'rack_id': transfer_data.from_rack_id
        }, {'_id': 0})
        
        if not from_assignment:
            raise HTTPException(status_code=404, detail="Product not found in source rack")
        
        if from_assignment['quantity'] < transfer_data.quantity:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient quantity in source rack. Available: {from_assignment['quantity']}"
            )
        
        # Deduct from source rack
        new_quantity = from_assignment['quantity'] - transfer_data.quantity
        if new_quantity == 0:
            await db.rack_assignments.delete_one({'id': from_assignment['id']})
        else:
            await db.rack_assignments.update_one(
                {'id': from_assignment['id']},
                {'$set': {'quantity': new_quantity, 'updated_at': datetime.now(timezone.utc).isoformat()}}
            )
        
        from_rack = await db.racks.find_one({'id': transfer_data.from_rack_id}, {'_id': 0})
        from_rack_code = from_rack['code'] if from_rack else None
    
    # Add to destination rack
    if transfer_data.to_rack_id:
        to_assignment = await db.rack_assignments.find_one({
            'product_id': transfer_data.product_id,
            'rack_id': transfer_data.to_rack_id
        }, {'_id': 0})
        
        to_rack = await db.racks.find_one({'id': transfer_data.to_rack_id}, {'_id': 0})
        to_rack_code = to_rack['code'] if to_rack else None
        
        if to_assignment:
            # Update existing assignment
            new_quantity = to_assignment['quantity'] + transfer_data.quantity
            await db.rack_assignments.update_one(
                {'id': to_assignment['id']},
                {'$set': {'quantity': new_quantity, 'updated_at': datetime.now(timezone.utc).isoformat()}}
            )
        else:
            # Create new assignment
            if to_rack:
                assignment = RackAssignment(
                    product_id=transfer_data.product_id,
                    product_name=product['name'],
                    rack_id=transfer_data.to_rack_id,
                    rack_code=to_rack['code'],
                    location_id=to_rack['location_id'],
                    location_name=to_rack['location_name'],
                    quantity=transfer_data.quantity,
                    assigned_by=current_user.id,
                    assigned_by_name=current_user.name
                )
                doc = assignment.model_dump()
                doc['created_at'] = doc['created_at'].isoformat()
                doc['updated_at'] = doc['updated_at'].isoformat()
                await db.rack_assignments.insert_one(doc)
    
    # Create transfer log
    transfer = StockTransfer(
        product_id=transfer_data.product_id,
        product_name=product['name'],
        from_rack_id=transfer_data.from_rack_id,
        from_rack_code=from_rack_code,
        to_rack_id=transfer_data.to_rack_id,
        to_rack_code=to_rack_code,
        quantity=transfer_data.quantity,
        transfer_type=transfer_data.transfer_type,
        notes=transfer_data.notes,
        transferred_by=current_user.id,
        transferred_by_name=current_user.name
    )
    
    doc = transfer.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.stock_transfers.insert_one(doc)
    
    return transfer

# Product Location Lookup
@api_router.get("/products/{product_id}/locations")
async def get_product_locations(product_id: str, current_user: User = Depends(get_current_user)):
    product = await db.products.find_one({'id': product_id}, {'_id': 0})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    assignments = await db.rack_assignments.find({'product_id': product_id}, {'_id': 0}).to_list(1000)
    
    # Group by location
    mall_racks = []
    warehouse_racks = []
    
    for assignment in assignments:
        location = await db.locations.find_one({'id': assignment['location_id']}, {'_id': 0})
        if location:
            if location['type'] == 'mall':
                mall_racks.append(assignment)
            else:
                warehouse_racks.append(assignment)
    
    mall_total = sum(r['quantity'] for r in mall_racks)
    warehouse_total = sum(r['quantity'] for r in warehouse_racks)
    
    return {
        'product': product,
        'mall_racks': mall_racks,
        'warehouse_racks': warehouse_racks,
        'mall_total': mall_total,
        'warehouse_total': warehouse_total,
        'total_assigned': mall_total + warehouse_total
    }

# Dashboard Low Stock by Location
@api_router.get("/dashboard/low-stock-by-location")
async def get_low_stock_by_location(current_user: User = Depends(get_current_user)):
    products = await db.products.find({}, {'_id': 0}).to_list(1000)
    locations = await db.locations.find({}, {'_id': 0}).to_list(10)
    
    mall_low_stock = []
    warehouse_low_stock = []
    
    for product in products:
        assignments = await db.rack_assignments.find({'product_id': product['id']}, {'_id': 0}).to_list(1000)
        
        mall_qty = 0
        warehouse_qty = 0
        
        for assignment in assignments:
            location = next((l for l in locations if l['id'] == assignment['location_id']), None)
            if location:
                if location['type'] == 'mall':
                    mall_qty += assignment['quantity']
                else:
                    warehouse_qty += assignment['quantity']
        
        # Default thresholds
        mall_threshold = 5
        warehouse_threshold = 20
        
        if mall_qty <= mall_threshold and mall_qty > 0:
            mall_low_stock.append({
                **product,
                'current_quantity': mall_qty,
                'threshold': mall_threshold
            })
        
        if warehouse_qty <= warehouse_threshold and warehouse_qty > 0:
            warehouse_low_stock.append({
                **product,
                'current_quantity': warehouse_qty,
                'threshold': warehouse_threshold
            })
    
    return {
        'mall_low_stock': mall_low_stock[:10],
        'warehouse_low_stock': warehouse_low_stock[:10],
        'mall_count': len(mall_low_stock),
        'warehouse_count': len(warehouse_low_stock)
    }

app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# @app.on_event("shutdown")


app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
async def shutdown_db_client():
    client.close()