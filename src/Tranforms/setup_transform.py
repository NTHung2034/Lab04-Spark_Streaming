#!/usr/bin/env python3
"""
Setup script for Transform Phase Kafka topics
This script creates the required Kafka topics for the Transform phase
"""

import subprocess
import sys
import time

def run_command(command, description):
    """Run a shell command and handle errors"""
    print(f"\n{description}...")
    print(f"Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Success: {description}")
            if result.stdout.strip():
                print(f"Output: {result.stdout.strip()}")
        else:
            print(f"❌ Error: {description}")
            print(f"Error output: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ Exception during {description}: {e}")
        return False
    
    return True

def check_docker_containers():
    """Check if Kafka containers are running"""
    print("Checking Docker containers...")
    
    # List running containers
    result = subprocess.run("docker ps --format 'table {{.Names}}\t{{.Status}}'", 
                          shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("Running containers:")
        print(result.stdout)
        
        # Check if Kafka container exists
        if "kafka" in result.stdout.lower():
            print("✅ Kafka container found")
            return True
        else:
            print("❌ No Kafka container found")
            return False
    else:
        print("❌ Failed to check Docker containers")
        return False

def get_kafka_container_name():
    """Try to find the Kafka container name"""
    result = subprocess.run("docker ps --format '{{.Names}}' | grep -i kafka", 
                          shell=True, capture_output=True, text=True)
    
    if result.returncode == 0 and result.stdout.strip():
        container_name = result.stdout.strip().split('\n')[0]
        print(f"Found Kafka container: {container_name}")
        return container_name
    
    # Try common container names
    common_names = ["extract_kafka_1", "kafka", "kafka-container", "kafka_kafka_1"]
    for name in common_names:
        result = subprocess.run(f"docker ps --filter name={name} --format '{{.Names}}'", 
                              shell=True, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            print(f"Found Kafka container: {name}")
            return name
    
    print("❌ Could not find Kafka container automatically")
    print("Please provide the Kafka container name manually")
    return None

def list_topics(container_name):
    """List existing Kafka topics"""
    command = f"docker exec -it {container_name} /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list"
    return run_command(command, "Listing existing topics")

def create_topic(container_name, topic_name, partitions=3, replication_factor=1):
    """Create a Kafka topic"""
    command = (f"docker exec -it {container_name} /usr/bin/kafka-topics "
              f"--bootstrap-server localhost:9092 --create "
              f"--topic {topic_name} --partitions {partitions} "
              f"--replication-factor {replication_factor}")
    
    return run_command(command, f"Creating topic '{topic_name}'")

def delete_topic(container_name, topic_name):
    """Delete a Kafka topic"""
    command = (f"docker exec -it {container_name} /usr/bin/kafka-topics "
              f"--bootstrap-server localhost:9092 --delete --topic {topic_name}")
    
    return run_command(command, f"Deleting topic '{topic_name}'")

def main():
    print("🚀 Transform Phase Kafka Topics Setup")
    print("=" * 50)
    
    # Check Docker containers
    if not check_docker_containers():
        print("\n❌ Please make sure your Kafka Docker containers are running")
        sys.exit(1)
    
    # Get Kafka container name
    container_name = get_kafka_container_name()
    if not container_name:
        container_name = input("Please enter your Kafka container name (e.g., extract_kafka_1): ").strip()
        if not container_name:
            print("❌ Container name is required")
            sys.exit(1)
    
    print(f"\nUsing Kafka container: {container_name}")
    
    # List existing topics
    print("\n" + "=" * 30)
    print("EXISTING TOPICS")
    print("=" * 30)
    list_topics(container_name)
    
    # Topics needed for Transform phase
    required_topics = [
        ("btc-price", "Input topic from Extract phase"),
        ("btc-price-moving", "Output topic for moving statistics"),
        ("btc-price-zscore", "Output topic for Z-scores"),
        ("btc-price-higher", "Bonus: Higher price windows"),
        ("btc-price-lower", "Bonus: Lower price windows")
    ]
    
    print("\n" + "=" * 30)
    print("CREATING TRANSFORM TOPICS")
    print("=" * 30)
    
    success_count = 0
    for topic_name, description in required_topics:
        print(f"\n📝 {topic_name}: {description}")
        if create_topic(container_name, topic_name):
            success_count += 1
        time.sleep(1)  # Small delay between topic creations
    
    print("\n" + "=" * 30)
    print("SETUP SUMMARY")
    print("=" * 30)
    
    if success_count == len(required_topics):
        print("✅ All topics created successfully!")
    else:
        print(f"⚠️  {success_count}/{len(required_topics)} topics created successfully")
        print("Some topics may already exist, which is normal")
    
    # List topics again to confirm
    print("\n" + "=" * 30)
    print("FINAL TOPIC LIST")
    print("=" * 30)
    list_topics(container_name)
    
    print("\n🎉 Transform phase setup complete!")
    print("\nNext steps:")
    print("1. Run: python moving_statistics.py")
    print("2. Run: python zscore_calculator.py")
    print("3. Test with: python test_transform_consumer.py")

if __name__ == "__main__":
    main()